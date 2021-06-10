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
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.ImmutableList;
import io.spine.query.RecordColumn;
import io.spine.query.RecordQuery;
import io.spine.query.SortBy;
import io.spine.server.storage.datastore.Kind;

import java.util.function.Function;

import static com.google.cloud.datastore.StructuredQuery.OrderBy.asc;
import static com.google.cloud.datastore.StructuredQuery.OrderBy.desc;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.query.Direction.ASC;

/**
 * A function transforming the input {@link com.google.cloud.datastore.StructuredQuery.Filter
 * Filter} into a {@link com.google.cloud.datastore.StructuredQuery} with the given newBuilder.
 *
 * //TODO:2021-04-14:alex.tymchenko: rewrite this description! It makes no sense.
 */
final class QueryWithFilter implements Function<StructuredQuery.Filter, StructuredQuery<Entity>> {

    private final StructuredQuery.Builder<Entity> builder;

    QueryWithFilter(RecordQuery<?, ?> query, Kind kind) {
        checkNotNull(query);
        checkNotNull(kind);

        this.builder = Query.newEntityQueryBuilder()
                            .setKind(kind.value());
        ImmutableList<? extends SortBy<?, ?>> sorting = query.sorting();
        if (!sorting.isEmpty()) {
            for (SortBy<?, ?> sortBy : sorting) {
                StructuredQuery.OrderBy orderBy = translateSortBy(sortBy);
                builder.addOrderBy(orderBy);
            }
        }
        Integer limit = query.limit();
        if (limit != null && limit > 0) {
            this.builder.setLimit(limit);
        }
    }

    private static StructuredQuery.OrderBy translateSortBy(SortBy<?, ?> sortBy) {
        RecordColumn<?, ?> column = sortBy.column();
        String columnName = column.name()
                                  .value();
        return sortBy.direction() == ASC
               ? asc(columnName)
               : desc(columnName);
    }

    @Override
    public StructuredQuery<Entity> apply(StructuredQuery.Filter filter) {
        checkNotNull(filter);
        StructuredQuery<Entity> query = builder.setFilter(filter)
                                               .build();
        return query;
    }

    /**
     * Creates a new {@code StructuredQuery} without filters.
     */
    StructuredQuery<Entity> withNoFilter() {
        StructuredQuery<Entity> result = builder.build();
        return result;
    }
}
