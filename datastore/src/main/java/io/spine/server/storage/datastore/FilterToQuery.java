/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A function transforming the input {@link com.google.cloud.datastore.StructuredQuery.Filter}
 * into a {@link com.google.cloud.datastore.StructuredQuery} with the given newBuilder.
 */
final class FilterToQuery implements Function<StructuredQuery.Filter, StructuredQuery<Entity>> {

    private final StructuredQuery.Builder<Entity> builder;

    FilterToQuery(Kind kind) {
        this.builder = Query.newEntityQueryBuilder()
                            .setKind(kind.getValue());
    }

    @Override
    public StructuredQuery<Entity> apply(StructuredQuery.Filter filter) {
        checkNotNull(filter);
        StructuredQuery<Entity> query = builder.setFilter(filter)
                                               .build();
        return query;
    }
}
