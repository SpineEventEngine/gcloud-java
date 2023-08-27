/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.spine.client.Filter;
import io.spine.query.ComparisonOperator;
import io.spine.query.QueryPredicate;
import io.spine.query.SubjectParameter;
import io.spine.server.storage.ColumnMapping;

import java.util.Collection;
import java.util.List;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.ge;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.le;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.lt;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.spine.query.LogicalOperator.AND;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

/**
 * A utility for working with the Datastore {@linkplain Filter filters}.
 */
final class DsFilters {

    /**
     * Prevents the utility class instantiation.
     */
    private DsFilters() {
    }

    /**
     * Converts the given {@link QueryPredicate} instances into the Datastore
     * {@link Filter Filters}.
     *
     * <p>The returned {@code Collection} contains the same predicate as the passed
     * {@code Collection} of query parameters. The difference is in
     * <ul>
     *     <li>type: from a {@code QueryPredicate} to Datastore {@code Filter};
     *     <li>format: all the resulting {@code Filter}s are conjunctive (as Datastore does not
     *         support disjunction) and are related with each other in the disjunctive way.
     * </ul>
     *
     * <p><i>Example:</i>
     *
     * <p>Given query predicates {@code p1}, {@code p2}, {@code p3}, {@code p4}, {@code p5} passed
     * into the method within the following construction: {@code p1 & (p2 | p3) & (p4 | p5)}. Then
     * the resulting {@code Collection} of {@code Filters} will be constructed as
     * {@code (p1 & p2 & p4) | (p1 & p2 & p5) | (p1 & p3 & p4) | (p1 & p3 & p5)}.
     *
     * <p>The separate conjunctive groups (e.g. {@code (p1 & p2 & p4)}) in the result
     * {@code Collection} are placed into a single {@code Filter} instances one per group.
     *
     * <p>In other words, the predicate expression is brought into the
     * <a href="https://en.wikipedia.org/wiki/Disjunctive_normal_form">disjunctive normal form</a>.
     *
     * <p>Note that by the convention, the distinct {@code QueryPredicate} instances
     * are considered to be joined by the {@code &} operator. However, the resulting {@code Filter}
     * instances are specified to be joined with {@code |} operator, so that the merged result
     * of executing the queries with each of the filters will be the result of the whole expression.
     *
     * <p>If the given parameter {@code Collection} is empty, and empty {@code Collection}
     * is returned.
     *
     * @param predicate
     *         the predicate to convert
     * @param adapter
     *         an adapter performing the required type conversions
     * @return the equivalent expression of in Datastore {@code Filter} instances
     */
    static <R extends Message> Collection<StructuredQuery.Filter>
    fromPredicate(QueryPredicate<R> predicate, FilterAdapter adapter) {
        checkNotNull(predicate);
        checkNotNull(adapter);

        Collection<StructuredQuery.Filter> results;
        if (predicate.isEmpty()) {
            results = emptySet();
        } else {
            results = toDsFilters(predicate, adapter);
        }
        return results;
    }

    private static <R extends Message> Collection<StructuredQuery.Filter>
    toDsFilters(QueryPredicate<R> predicate, FilterAdapter adapter) {
        ImmutableList.Builder<StructuredQuery.Filter> result = ImmutableList.builder();
        var dnf = predicate.toDnf();
        var mapping = adapter.columnMapping();
        if (dnf.operator() == AND) {
            var group = handleConjunctiveGroup(dnf, mapping);
            result.add(group);
        } else {
            handleParameters(dnf.allParams(), mapping, result);
            var children = dnf.children();
            for (var child : children) {
                var group = handleConjunctiveGroup(child, mapping);
                result.add(group);
            }
        }
        return result.build();
    }

    /**
     * Converts each of the passed parameter to a distinct {@link StructuredQuery.Filter} according
     * to the passed column mapping, and appends it to the result builder.
     */
    private static void handleParameters(ImmutableList<SubjectParameter<?, ?, ?>> parameters,
                                         ColumnMapping<Value<?>> mapping,
                                         ImmutableList.Builder<StructuredQuery.Filter> result) {
        var filters = parameters.stream()
                .map(param -> createFilter(param, mapping))
                .collect(toImmutableList());
        result.addAll(filters);
    }

    private static <R extends Message> StructuredQuery.Filter
    handleConjunctiveGroup(QueryPredicate<R> predicate, ColumnMapping<Value<?>> mapping) {
        checkState(predicate.children()
                            .isEmpty(),
                   "Children collection must be empty for a conjunctive predicate group.");
        var parameters = predicate.allParams();
        List<StructuredQuery.Filter> filters = parameters.stream()
                        .map(param -> createFilter(param, mapping))
                        .collect(toList());

        checkState(!filters.isEmpty());
        var first = filters.get(0);
        var other = skipFirst(filters);
        StructuredQuery.Filter group = and(first, other);
        return group;
    }

    /**
     * Returns the contents of the passed list as a new array, but does not include
     * the first element of the list into the result.
     */
    private static StructuredQuery.Filter[] skipFirst(List<StructuredQuery.Filter> filters) {
        var size = filters.size();
        var sublist = filters.subList(1, size);
        var result = sublist.toArray(new StructuredQuery.Filter[size - 1]);
        return result;
    }

    private static PropertyFilter
    createFilter(SubjectParameter<?, ?, ?> parameter, ColumnMapping<Value<?>> mapping) {
        checkNotNull(parameter);
        checkNotNull(mapping);
        var paramValue = parameter.value();
        var typeMapping = mapping.of(paramValue.getClass());
        var value = typeMapping.applyTo(paramValue);
        var columnName = parameter.column()
                                  .name()
                                  .value();
        var result = asFilter(columnName, parameter.operator(), value);
        return result;
    }

    private static PropertyFilter
    asFilter(String column, ComparisonOperator operator, Value<?> value) {
        switch (operator) {
            case EQUALS:
                return eq(column, value);
            case GREATER_THAN:
                return gt(column, value);
            case LESS_THAN:
                return lt(column, value);
            case GREATER_OR_EQUALS:
                return ge(column, value);
            case LESS_OR_EQUALS:
                return le(column, value);
            default:
                throw new IllegalStateException(operator.name());
        }
    }
}
