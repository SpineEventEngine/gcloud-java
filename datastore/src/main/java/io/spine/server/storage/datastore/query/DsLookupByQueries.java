/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.spine.server.storage.datastore.query;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.query.QueryPredicate;
import io.spine.query.RecordQuery;
import io.spine.query.SortBy;
import io.spine.server.storage.datastore.DatastoreMedium;
import io.spine.server.storage.datastore.DsQueryIterator;
import io.spine.server.storage.datastore.Kind;
import io.spine.server.storage.datastore.record.DsEntitySpec;
import io.spine.server.storage.datastore.record.Entities;
import io.spine.server.storage.datastore.record.FieldMaskApplier;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.spine.server.storage.datastore.record.DsEntityComparator.implementing;
import static java.util.stream.Collectors.toList;

/**
 * An {@code Entity} lookup using {@linkplain QueryPredicate Spine query predicates}.
 *
 * @implNote Due to Datastore restrictions, execution of a single
 *         {@link io.spine.query.Query Query} may result into several Datastore reads.
 *         See {@link DsFilters} for details.
 */
final class DsLookupByQueries<I, R extends Message> extends PreparedQuery<I, R> {

    private static final int MISSING_LIMIT = 0;

    private final DatastoreMedium datastore;

    //TODO:2021-02-28:alex.tymchenko: document!
    private final @Nullable Filter ancestorFilter;

    /**
     * A converter from {@link Entity} to {@code <R>} instances.
     *
     * <p>Initialized upon fetching the entities from the Datastore.
     */
    private @MonotonicNonNull ToRecords<R> transformer = null;

    DsLookupByQueries(DatastoreMedium datastore,
                      RecordQuery<I, R> query,
                      FilterAdapter columnAdapter,
                      DsEntitySpec<I, R> spec) {
        super(query, columnAdapter, spec);
        this.datastore = datastore;
        this.ancestorFilter = ancestorFilter(query, datastore);
    }

    private @Nullable Filter ancestorFilter(RecordQuery<I, R> query, DatastoreMedium datastore) {
        Optional<Filter> maybeFilter = spec().layout()
                                             .ancestorFilter(query, datastore);
        return maybeFilter.orElse(null);
    }

    @Override
    protected IntermediateResult fetchFromDatastore() {
        ImmutableList<Entity> rawEntities = findByPredicates(query());
        return new IntermediateResult(rawEntities);
    }

    @Override
    protected Iterable<R> toRecords(IntermediateResult result) {
        checkNotNull(transformer,
                     "In-memory transformer `Datastore Entity`->`Stored record` isn't set.");
        Iterable<R> records = transformer.apply(result);
        return records;
    }

    private ImmutableList<Entity> findByPredicates(RecordQuery<?, R> query) {
        ImmutableList<Entity> results;
        List<StructuredQuery<Entity>> queries = split(query);
        if (queries.size() == 1) {
            results = findForSingle(queries.get(0));
            transformer = new ConvertAsIs<>(recordType(), mask());
        } else {
            results = readAndJoin(queries);
            transformer = new SortAndLimit<>(recordType(), mask(), sorting(), limit());
        }

        return results;
    }

    private List<StructuredQuery<Entity>> split(RecordQuery<?, R> query) {
        QueryPredicate<R> rootPredicate = query.subject()
                                                           .predicate();
        Kind kind = spec().kind();
        if(rootPredicate.isEmpty()) {
            StructuredQuery<Entity> result = new QueryWithFilter(query, kind).withNoFilter();
            return ImmutableList.of(result);
        }

        List<StructuredQuery<Entity>> queries = toDatastoreFilters(rootPredicate)
                .stream()
                .map(new QueryWithFilter(query, kind))
                .collect(toList());
        return queries;
    }

    private Collection<Filter>
    toDatastoreFilters(QueryPredicate<R> rootPredicate) {
        Collection<Filter> dsFilters =
                DsFilters.fromPredicate(rootPredicate, columnAdapter());
        return dsFilters;
    }

    private ImmutableList<Entity> findForSingle(StructuredQuery<Entity> query) {
        StructuredQuery<Entity> adjustedForLayout = adjustForLayout(query);
        DsQueryIterator<Entity> iterator = datastore.read(adjustedForLayout);
        ImmutableList<Entity> result = ImmutableList.copyOf(iterator);
        return result;
    }

    private StructuredQuery<Entity> adjustForLayout(StructuredQuery<Entity> query) {
        if(ancestorFilter == null) {
            return query;
        }
        Filter filter = query.getFilter();
        CompositeFilter filterWithNesting = CompositeFilter.and(filter, ancestorFilter);

        StructuredQuery<Entity> result = query.toBuilder()
                                              .setFilter(filterWithNesting)
                                              .build();
        return result;
    }

    /**
     * Runs multiple Datastore queries in series.
     *
     * <p>Joins the results of each query into a single {@code ImmutableList}. Duplicate entities
     * are filtered out.
     *
     * <p>Each query is run without the {@code limit} set.
     */
    private ImmutableList<Entity> readAndJoin(Collection<StructuredQuery<Entity>> queries) {
        ImmutableList<Entity> entities =
                queries.stream()
                       .map(DsLookupByQueries::clearLimit)
                       .map(this::adjustForLayout)
                       .map(datastore::read)
                       .flatMap(Streams::stream)
                       .distinct()
                       .collect(toImmutableList());
        return entities;
    }

    private static <R> StructuredQuery<R> clearLimit(StructuredQuery<R> q) {
        return q.toBuilder()
                .setLimit(Integer.MAX_VALUE)
                .build();
    }

    private abstract static class ToRecords<R extends Message>
            implements Function<IntermediateResult, Iterable<R>> {

        private final TypeUrl recordType;
        private final FieldMask mask;

        protected ToRecords(TypeUrl type, FieldMask mask) {
            recordType = type;
            this.mask = mask;
        }

        @Override
        public Iterable<R> apply(IntermediateResult result) {
            List<@Nullable Entity> entities = result.entities();
            Stream<Entity> stream =
                    entities.stream()
                            .filter(Objects::nonNull);
            stream = filter(stream);
            ImmutableList<R> records =
                    stream.map(this::toRecord)
                          .map(this::mask)
                          .collect(toImmutableList());
            return records;
        }

        private R mask(R r) {
            return FieldMaskApplier.<R>recordMasker(mask).apply(r);
        }

        private R toRecord(Entity e) {
            return Entities.toMessage(e, recordType);
        }

        protected abstract Stream<Entity> filter(Stream<Entity> entities);
    }

    /**
     * Converts the original {@code Entity} instances into the records of {@code <R>}
     * and applies the field mask to each of them.
     *
     * <p>Performs no intermediate filtering.
     *
     * @param <R>
     *         the type of the records to convert each {@code Entity} into
     */
    private static final class ConvertAsIs<R extends Message> extends ToRecords<R> {

        private ConvertAsIs(TypeUrl type, FieldMask mask) {
            super(type, mask);
        }

        @Override
        protected Stream<Entity> filter(Stream<Entity> entities) {
            return entities;
        }
    }

    /**
     * Sorts and limits the original list of {@code Entity} objects, then converts each of them
     * to the {@code <R>}-typed records and applies the specified field mask to each of them.
     *
     * @param <R>
     *         the type of the records to convert each {@code Entity} into
     */
    private static final class SortAndLimit<R extends Message> extends ToRecords<R> {

        private final ImmutableList<SortBy<?, R>> sorting;
        private final @Nullable Integer limit;

        private SortAndLimit(TypeUrl type, FieldMask mask,
                             ImmutableList<SortBy<?, R>> sorting,
                             @Nullable Integer limit) {
            super(type, mask);
            this.sorting = sorting;
            this.limit = limit;
        }

        @Override
        protected Stream<Entity> filter(Stream<Entity> entities) {

            Stream<Entity> currentStream = entities;
            if (!sorting.isEmpty()) {
                currentStream = currentStream.sorted(implementing(sorting));
            }
            if (limit != null && limit != MISSING_LIMIT) {
                currentStream = currentStream.limit(limit);
            }
            return currentStream;
        }
    }

//
//    /**
//     * Finds a collection of entities matching provided {@link QueryParameters} in Datastore and
//     * returns them according to the specified {@code format}.
//     *
//     * @param params
//     *         parameters specifying the filters for records to conform to
//     * @param format
//     *         format of the search specifying limits, order and field mask to apply to the results
//     * @return an iterator over the entity records from the Datastore
//     */
//    Iterator<EntityRecord> find(QueryParameters params, ResponseFormat format) {
//        List<StructuredQuery<Entity>> queries = splitToMultipleDsQueries(params, format);
//        FieldMask mask = format.getFieldMask();
//        if (queries.size() == 1) {
//            Iterator<EntityRecord> results = find(queries.get(0), mask);
//            return results;
//        }
//
//        Iterator<EntityRecord> results =
//                find(queries, format.getOrderBy(), format.getLimit(), mask);
//        return results;
//    }
//
//    private List<StructuredQuery<Entity>>
//    splitToMultipleDsQueries(QueryParameters params, ResponseFormat format) {
//        checkNotNull(params);
//
//        List<StructuredQuery<Entity>> queries = buildDsFilters(params.iterator())
//                .stream()
//                .map(new QueryWithFilter(format, Kind.of(typeUrl)))
//                .collect(toList());
//        return queries;
//    }
//
//    private Collection<StructuredQuery.Filter>
//    buildDsFilters(Iterator<CompositeQueryParameter> compositeParameters) {
//        Collection<CompositeQueryParameter> params = newArrayList(compositeParameters);
//        Collection<StructuredQuery.Filter> predicate =
//                DsFilters.fromParams(params, columnFilterAdapter);
//        return predicate;
//    }
//
//    /**
//     * Performs the given Datastore {@linkplain com.google.cloud.datastore.StructuredQuery queries}
//     * and combines results into a single lazy iterator applying the field mask to each item.
//     *
//     * @param query
//     *         a query to perform
//     * @param fieldMask
//     *         a {@code FieldMask} to apply to all the retrieved entity states
//     * @return an iterator over the resulting entity records
//     */
//    private Iterator<EntityRecord> find(StructuredQuery<Entity> query, FieldMask fieldMask) {
//        return find(singleton(query), OrderBy.getDefaultInstance(), MISSING_LIMIT, fieldMask);
//    }
//
//    /**
//     * Performs the given Datastore {@linkplain com.google.cloud.datastore.StructuredQuery queries}
//     * and combines results into a single iterator applying the field mask to each item.
//     *
//     * <p>Provided {@link OrderBy inMemOrderBy} is applied to the combined results of Datastore
//     * reads and sorts them in-memory. Otherwise the read results will be lazy.
//     *
//     * @param queries
//     *         queries to perform
//     * @param inMemOrderBy
//     *         an order by which the retrieved entities are sorted in-memory
//     * @param limit
//     *         an integer limit of number of records to be returned
//     * @param fieldMask
//     *         a {@code FieldMask} to apply to all the retrieved entity states
//     * @return an iterator over the resulting entity records
//     */
//    private Iterator<EntityRecord> find(Collection<StructuredQuery<Entity>> queries,
//                                        OrderBy inMemOrderBy, int limit, FieldMask fieldMask) {
//        checkNotNull(queries);
//        checkNotNull(inMemOrderBy);
//        checkNotNull(fieldMask);
//        checkArgument(limit >= 0, "A query limit cannot be negative.");
//        checkArgument(queries.size() > 0, "At least one query is required.");
//
//        Stream<Entity> entities = readAndJoin(queries);
//
//        if (!isDefault(inMemOrderBy)) {
//            entities = entities.sorted(implementing(inMemOrderBy));
//        }
//        if (limit != MISSING_LIMIT) {
//            entities = entities.limit(limit);
//        }
//
//        Iterator<EntityRecord> recordIterator =
//                entities.map(Entities::toRecord)
//                        .map(recordMasker(fieldMask))
//                        .iterator();
//
//        return recordIterator;
//    }
}
