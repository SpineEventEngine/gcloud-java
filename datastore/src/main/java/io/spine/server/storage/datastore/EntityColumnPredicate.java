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
import com.google.common.collect.Multimap;
import io.spine.client.ColumnFilter;
import io.spine.client.CompositeColumnFilter;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityColumn;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.function.Predicate;

import static io.spine.server.storage.OperatorEvaluator.eval;
import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * Test if a Datastore entity matches query parameters.
 */
final class EntityColumnPredicate implements Predicate<Entity> {

    private final ColumnFilterAdapter adapter;
    private final Iterable<CompositeQueryParameter> queryParams;

    EntityColumnPredicate(Iterable<CompositeQueryParameter> queryParams,
                          ColumnFilterAdapter adapter) {
        this.adapter = adapter;
        this.queryParams = queryParams;
    }

    @Override
    public boolean test(@Nullable Entity entity) {
        if (entity == null) {
            return false;
        }
        for (CompositeQueryParameter filter : queryParams) {
            boolean match;
            CompositeColumnFilter.CompositeOperator operator = filter.getOperator();
            switch (operator) {
                case ALL:
                    match = checkAll(filter.getFilters(), entity);
                    break;
                case EITHER:
                    match = checkEither(filter.getFilters(), entity);
                    break;
                case UNRECOGNIZED:      // Fall through to default strategy
                case CCF_CO_UNDEFINED:  // for the `default` and `faulty` enum values.
                default:
                    throw newIllegalArgumentException("Composite operator %s is invalid.",
                                                      operator);
            }
            if (!match) {
                return false;
            }
        }
        return true;
    }

    private boolean checkAll(Multimap<EntityColumn, ColumnFilter> filters, Entity entity) {
        for (Map.Entry<EntityColumn, ColumnFilter> filter : filters.entries()) {
            EntityColumn column = filter.getKey();
            boolean matches = checkSingleParam(filter.getValue(), entity, column);
            if (!matches) {
                return false;
            }
        }
        return true;
    }

    private boolean checkEither(Multimap<EntityColumn, ColumnFilter> filters, Entity entity) {
        for (Map.Entry<EntityColumn, ColumnFilter> filter : filters.entries()) {
            EntityColumn column = filter.getKey();
            boolean matches = checkSingleParam(filter.getValue(), entity, column);
            if (matches) {
                return true;
            }
        }
        return filters.isEmpty();
    }

    private boolean checkSingleParam(ColumnFilter filter, Entity entity, EntityColumn column) {
        String columnName = column.getName();
        if (!entity.contains(columnName)) {
            return false;
        }
        Object actual = entity.getValue(columnName)
                              .get();
        Object expected = adapter.toValue(column, filter)
                                 .get();

        boolean result = eval(actual, filter.getOperator(), expected);
        return result;
    }
}
