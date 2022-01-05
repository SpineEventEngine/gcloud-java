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

package io.spine.server.storage.datastore.query;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.ImmutableList;
import io.spine.query.RecordQuery;
import io.spine.query.SortBy;
import io.spine.server.storage.datastore.Kind;

import java.util.function.Function;

import static com.google.cloud.datastore.StructuredQuery.OrderBy.asc;
import static com.google.cloud.datastore.StructuredQuery.OrderBy.desc;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.query.Direction.ASC;

/**
 * A template of a Datastore's {@code Query} built upon some generic properties of the passed
 * {@code RecordQuery} (such as sorting and limit) and awaiting a Datastore's native {@code Filter}
 * — supplied by other routines — to produce an instance of {@code StructuredQuery} ready
 * for execution against Datastore.
 *
 * <p>The transformation of {@code RecordQuery} into one or more {@code StructuredQuery}
 * instances is a complex process. Several different parties are involved into it, including
 * {@linkplain DsFilters utility} producing the Datastore {@code Filters}.
 *
 * <p>This type serves a "staging area" by accumulating the pieces required
 * to build a proper {@code StructuredQuery}.
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
                var orderBy = translateSortBy(sortBy);
                builder.addOrderBy(orderBy);
            }
        }
        var limit = query.limit();
        if (limit != null && limit > 0) {
            this.builder.setLimit(limit);
        }
    }

    private static StructuredQuery.OrderBy translateSortBy(SortBy<?, ?> sortBy) {
        var column = sortBy.column();
        var columnName = column.name()
                               .value();
        return sortBy.direction() == ASC
               ? asc(columnName)
               : desc(columnName);
    }

    @Override
    public StructuredQuery<Entity> apply(StructuredQuery.Filter filter) {
        checkNotNull(filter);
        var query = builder.setFilter(filter)
                           .build();
        return query;
    }

    /**
     * Creates a new {@code StructuredQuery} without filters.
     */
    StructuredQuery<Entity> withNoFilter() {
        var result = builder.build();
        return result;
    }
}
