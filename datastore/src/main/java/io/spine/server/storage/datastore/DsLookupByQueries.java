/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.Streams;
import com.google.protobuf.FieldMask;
import io.spine.client.OrderBy;
import io.spine.client.ResponseFormat;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.type.TypeUrl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static io.spine.protobuf.Messages.isDefault;
import static io.spine.server.storage.datastore.DsEntityComparator.implementing;
import static io.spine.server.storage.datastore.FieldMaskApplier.recordMasker;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

/**
 * An {@code Entity} lookup using {@linkplain QueryParameters Spine query parameters}.
 *
 * @implNote A single {@linkplain #find(QueryParameters, ResponseFormat) find()} call may
 *         turn into several Datastore reads. See {@link DsFilters} for details.
 */
final class DsLookupByQueries {

    private static final int MISSING_LIMIT = 0;

    private final DatastoreWrapper datastore;
    private final TypeUrl typeUrl;
    private final FilterAdapter columnFilterAdapter;

    DsLookupByQueries(DatastoreWrapper datastore, TypeUrl typeUrl,
                      FilterAdapter columnFilterAdapter) {
        this.datastore = datastore;
        this.typeUrl = typeUrl;
        this.columnFilterAdapter = columnFilterAdapter;
    }

    /**
     * Finds a collection of entities matching provided {@link QueryParameters} in Datastore and
     * returns them according to the specified {@code format}.
     *
     * @param params
     *         parameters specifying the filters for records to conform to
     * @param format
     *         format of the search specifying limits, order and field mask to apply to the results
     * @return an iterator over the entity records from the Datastore
     */
    Iterator<EntityRecord> find(QueryParameters params, ResponseFormat format) {
        List<StructuredQuery<Entity>> queries = splitToMultipleDsQueries(params, format);
        FieldMask mask = format.getFieldMask();
        if (queries.size() == 1) {
            Iterator<EntityRecord> results = find(queries.get(0), mask);
            return results;
        }

        Iterator<EntityRecord> results =
                find(queries, format.getOrderBy(), format.getLimit(), mask);
        return results;
    }

    private List<StructuredQuery<Entity>>
    splitToMultipleDsQueries(QueryParameters params, ResponseFormat format) {
        checkNotNull(params);

        List<StructuredQuery<Entity>> queries = buildDsFilters(params.iterator())
                .stream()
                .map(new QueryWithFilter(format, Kind.of(typeUrl)))
                .collect(toList());
        return queries;
    }

    private Collection<StructuredQuery.Filter>
    buildDsFilters(Iterator<CompositeQueryParameter> compositeParameters) {
        Collection<CompositeQueryParameter> params = newArrayList(compositeParameters);
        Collection<StructuredQuery.Filter> predicate =
                DsFilters.fromParams(params, columnFilterAdapter);
        return predicate;
    }

    /**
     * Performs the given Datastore {@linkplain com.google.cloud.datastore.StructuredQuery queries}
     * and combines results into a single lazy iterator applying the field mask to each item.
     *
     * @param query
     *         a query to perform
     * @param fieldMask
     *         a {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> find(StructuredQuery<Entity> query, FieldMask fieldMask) {
        return find(singleton(query), OrderBy.getDefaultInstance(), MISSING_LIMIT, fieldMask);
    }

    /**
     * Performs the given Datastore {@linkplain com.google.cloud.datastore.StructuredQuery queries}
     * and combines results into a single iterator applying the field mask to each item.
     *
     * <p>Provided {@link OrderBy inMemOrderBy} is applied to the combined results of Datastore
     * reads and sorts them in-memory. Otherwise the read results will be lazy.
     *
     * @param queries
     *         queries to perform
     * @param inMemOrderBy
     *         an order by which the retrieved entities are sorted in-memory
     * @param limit
     *         an integer limit of number of records to be returned
     * @param fieldMask
     *         a {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> find(Collection<StructuredQuery<Entity>> queries,
                                        OrderBy inMemOrderBy, int limit, FieldMask fieldMask) {
        checkNotNull(queries);
        checkNotNull(inMemOrderBy);
        checkNotNull(fieldMask);
        checkArgument(limit >= 0, "A query limit cannot be negative.");
        checkArgument(queries.size() > 0, "At least one query is required.");

        Stream<Entity> entities = read(queries);

        if (!isDefault(inMemOrderBy)) {
            entities = entities.sorted(implementing(inMemOrderBy));
        }
        if (limit != MISSING_LIMIT) {
            entities = entities.limit(limit);
        }

        Iterator<EntityRecord> recordIterator =
                entities.map(Entities::toRecord)
                        .map(recordMasker(fieldMask))
                        .iterator();

        return recordIterator;
    }

    /**
     * Runs multiple Datastore queries in series.
     *
     * <p>In case of overlapping results, only one instance of {@code Entity} is taken
     * (i.e. no duplications).
     */
    private Stream<Entity> read(Collection<StructuredQuery<Entity>> queries) {
        Stream<Entity> entities = queries
                .stream()
                .map(datastore::read)
                .flatMap(Streams::stream)
                .distinct();
        return entities;
    }
}
