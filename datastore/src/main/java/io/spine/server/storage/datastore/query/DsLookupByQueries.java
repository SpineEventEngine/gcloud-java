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
import com.google.protobuf.Message;
import io.spine.query.QueryPredicate;
import io.spine.query.RecordQuery;
import io.spine.server.storage.datastore.DatastoreMedium;
import io.spine.server.storage.datastore.DsQueryIterator;
import io.spine.server.storage.datastore.Kind;
import io.spine.server.storage.datastore.record.DsEntitySpec;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * An {@code Entity} lookup using {@linkplain QueryPredicate Spine query predicates}.
 *
 * @implNote Due to Datastore restrictions, execution of a single
 *         {@link io.spine.query.Query Query} may result into several Datastore reads.
 *         See {@link DsFilters} for details.
 */
final class DsLookupByQueries<I, R extends Message> extends PreparedQuery<I, R> {

    private final DatastoreMedium datastore;

    /**
     * An ancestor filter specific to the record layout according to which the queried records
     * are stored.
     *
     * <p>If the records are stored flat (i.e., no ancestor-child hierarchy is used)
     * this filter is {@code null}.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<Filter> ancestorFilter;

    /**
     * A converter from {@link Entity} to {@code <R>} instances.
     *
     * <p>Initialized upon fetching the entities from the Datastore.
     */
    private @MonotonicNonNull ToRecords<R> transformer = null;

    /**
     * Creates a new lookup for the passed {@code RecordQuery}.
     *
     * @param datastore
     *         Datastore connector
     * @param query
     *         a query to create this lookup for
     * @param columnAdapter
     *         an adapter of {@code RecordQuery} parameter values to Datastore-native types
     * @param spec
     *         Entity specification of the queried records
     */
    DsLookupByQueries(DatastoreMedium datastore,
                      RecordQuery<I, R> query,
                      FilterAdapter columnAdapter,
                      DsEntitySpec<I, R> spec) {
        super(query, columnAdapter, spec);
        this.datastore = datastore;
        this.ancestorFilter = ancestorFilter(query, datastore);
    }

    private Optional<Filter> ancestorFilter(RecordQuery<I, R> query, DatastoreMedium datastore) {
        Optional<Filter> result = spec().layout()
                                        .ancestorFilter(query, datastore);
        return result;
    }

    @Override
    IntermediateResult fetchFromDatastore() {
        ImmutableList<Entity> rawEntities = findByPredicates(query());
        return new IntermediateResult(rawEntities);
    }

    @Override
    Iterable<R> toRecords(IntermediateResult result) {
        checkNotNull(transformer,
                     "In-memory transformer `Datastore Entity`->`Stored record` isn't set.");
        Iterable<R> records = transformer.apply(result);
        return records;
    }

    private ImmutableList<Entity> findByPredicates(RecordQuery<?, R> query) {
        ImmutableList<Entity> results;
        List<StructuredQuery<Entity>> queries = split(query);
        if (queries.size() == 1) {
            results = runSingleQuery(queries.get(0));
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
        if (rootPredicate.isEmpty()) {
            StructuredQuery<Entity> result = new QueryWithFilter(query, kind).withNoFilter();
            return ImmutableList.of(result);
        }

        List<StructuredQuery<Entity>> queries = toDatastoreFilters(rootPredicate)
                .stream()
                .map(new QueryWithFilter(query, kind))
                .collect(toImmutableList());
        return queries;
    }

    private Collection<Filter> toDatastoreFilters(QueryPredicate<R> rootPredicate) {
        Collection<Filter> dsFilters = DsFilters.fromPredicate(rootPredicate, columnAdapter());
        return dsFilters;
    }

    private ImmutableList<Entity> runSingleQuery(StructuredQuery<Entity> query) {
        StructuredQuery<Entity> adjustedForLayout = adjustForLayout(query);
        DsQueryIterator<Entity> iterator = datastore.read(adjustedForLayout);
        ImmutableList<Entity> result = ImmutableList.copyOf(iterator);
        return result;
    }

    /**
     * Appends the Datastore's native ancestor filter, if the queried records are stored
     * in ancestor-child hierarchy.
     */
    private StructuredQuery<Entity> adjustForLayout(StructuredQuery<Entity> query) {
        if (!ancestorFilter.isPresent()) {
            return query;
        }
        Filter filter = query.getFilter();
        CompositeFilter filterWithNesting = CompositeFilter.and(filter, ancestorFilter.get());

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
}
