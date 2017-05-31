/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.Builder;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.cloud.datastore.Value;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Multimap;
import org.spine3.client.ColumnFilter;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.CompositeQueryParameter;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.ge;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.le;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.lt;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.spine3.client.CompositeColumnFilter.CompositeOperator.ALL;

/**
 * @author Dmytro Dashenkov
 */
final class DatastoreQueries {

    private static final Predicate<CompositeQueryParameter> isConjunctive =
            new Predicate<CompositeQueryParameter>() {
                @Override
                public boolean apply(@Nullable CompositeQueryParameter input) {
                    return input != null && input.getOperator() == ALL;
                }
            };

    private static final Predicate<CompositeQueryParameter> isDisjunctive = not(isConjunctive);

    private DatastoreQueries() {
        // Prevent utility class fromm being initialized.
    }

    static Collection<StructuredQuery<Entity>> fromColumnParameters(
            Collection<CompositeQueryParameter> parameters,
            ColumnHandler columnHandler,
            Kind kind) {
        checkNotNull(parameters);

        final Collection<StructuredQuery<Entity>> results;
        if (parameters.isEmpty()) {
            results = emptySet();
        } else {
            final Builder<Entity> template = Query.newEntityQueryBuilder()
                                                  .setKind(kind.getValue());
            final Iterable<Filter> filters = toFilters(parameters, columnHandler);
            results = concat(template, filters);
        }
        return results;
    }

    private static Collection<StructuredQuery<Entity>> concat(Builder<Entity> template,
                                                              Iterable<Filter> filters) {
        final Collection<StructuredQuery<Entity>> results = newLinkedList();
        for (Filter filter : filters) {
            final StructuredQuery<Entity> query = template.setFilter(filter)
                                                          .build();
            results.add(query);
        }
        return results;
    }

    private static Iterable<Filter> toFilters(Collection<CompositeQueryParameter> parameters,
                                              final ColumnHandler columnHandler) {
        final FluentIterable<CompositeQueryParameter> params = from(parameters);
        final FluentIterable<CompositeQueryParameter> conjunctionParams =
                params.filter(isConjunctive);
        final FluentIterable<CompositeQueryParameter> disjunctionParams =
                params.filter(isDisjunctive);
        final Optional<CompositeQueryParameter> firstParam = conjunctionParams.first();
        final Optional<CompositeQueryParameter> mergedConjunctiveParams =
                firstParam.transform(new Conjuncter(conjunctionParams.skip(1)));
        final Collection<Filter> filters = newLinkedList();
        ExpresionExpander.multiply(mergedConjunctiveParams.orNull(),
                                   disjunctionParams,
                                   new ColumnFiltersConjuncter(columnHandler, filters));
        return filters;
    }

    private static class Conjuncter implements Function<CompositeQueryParameter,
            CompositeQueryParameter> {

        private final Iterable<CompositeQueryParameter> otherParams;

        private Conjuncter(Iterable<CompositeQueryParameter> otherParams) {
            this.otherParams = otherParams;
        }

        @Override
        public CompositeQueryParameter apply(@Nullable CompositeQueryParameter input) {
            checkNotNull(input);
            final CompositeQueryParameter merged = input.conjunct(otherParams);
            return merged;
        }
    }

    private static class ColumnFiltersConjuncter implements ConjunctionProcessor {

        private final ColumnHandler columnHandler;
        private final Collection<Filter> filters;

        private ColumnFiltersConjuncter(ColumnHandler columnHandler,
                                        Collection<Filter> filters) {
            this.columnHandler = columnHandler;
            this.filters = filters;
        }

        @Override
        public void process(Iterable<AssembledColumnFilter> columnFilters) {
            Filter filter;
            final Iterator<AssembledColumnFilter> iterator = columnFilters.iterator();
            if (iterator.hasNext()) {
                filter = iterator.next()
                                 .toFilter(columnHandler);
            } else {
                return;
            }

            while (iterator.hasNext()) {
                final Filter propFilter = iterator.next()
                                                  .toFilter(columnHandler);
                filter = and(filter, propFilter);
            }
            filters.add(filter);
        }
    }

    private static class ExpresionExpander {

        private static void multiply(@Nullable CompositeQueryParameter constant,
                                     Iterable<CompositeQueryParameter> parameters,
                                     ConjunctionProcessor processor) {
            final AssembledColumnFilter expressionTree = buildConjunctionTree(constant,
                                                                              parameters);
            traverse(expressionTree, Collections.<AssembledColumnFilter>emptyList(), processor);
        }

        private static <T> Collection<T> newCollection(Collection<T> oldOne) {
            return new LinkedList<>(oldOne);
        }

        private static void traverse(AssembledColumnFilter node,
                                     Collection<AssembledColumnFilter> prefix,
                                     ConjunctionProcessor processor) {
            prefix.add(node);
            if (node.subtrees.isEmpty()) {
                processor.process(prefix);
            } else {
                for (AssembledColumnFilter successor : node.subtrees) {
                    traverse(successor, newCollection(prefix), processor);
                }
            }
        }

        private static AssembledColumnFilter buildConjunctionTree(
                @Nullable CompositeQueryParameter constant,
                Iterable<CompositeQueryParameter> parameters) {
            final AssembledColumnFilter lastSequentialNode;
            final AssembledColumnFilter head;
            if (constant != null) {
                final Collection<AssembledColumnFilter> filters = toFilters(constant);
                if (!filters.isEmpty()) {
                    AssembledColumnFilter prev = ColumnFilterGraphHead.instance();
                    head = prev;
                    for (AssembledColumnFilter filter : filters) {
                        prev.subtrees.add(filter);
                        prev = filter;
                    }
                    lastSequentialNode = prev;
                } else {
                    lastSequentialNode = ColumnFilterGraphHead.instance();
                    head = lastSequentialNode;
                }
            } else {
                lastSequentialNode = ColumnFilterGraphHead.instance();
                head = lastSequentialNode;
            }

            Collection<AssembledColumnFilter> lastLayer = singleton(lastSequentialNode);
            for (CompositeQueryParameter parameter : parameters) {
                final Collection<AssembledColumnFilter> currentLayer = toFilters(parameter);
                for (AssembledColumnFilter thisLayerItem : currentLayer) {
                    for (AssembledColumnFilter prevLayerItem : lastLayer) {
                        prevLayerItem.subtrees.add(thisLayerItem);
                    }
                }
                lastLayer = currentLayer;
            }
            return head;
        }

        private static Collection<AssembledColumnFilter> toFilters(CompositeQueryParameter param) {
            final Multimap<Column, ColumnFilter> srcFilters = param.getFilters();
            final Set<AssembledColumnFilter> filters = new HashSet<>(srcFilters.size());
            for (Map.Entry<Column, ColumnFilter> entry : srcFilters.entries()) {
                filters.add(new AssembledColumnFilter(entry.getKey(), entry.getValue()));
            }
            return filters;
        }
    }

    private static class AssembledColumnFilter {

        private final Column column;
        private final ColumnFilter columnFilter;

        private final Collection<AssembledColumnFilter> subtrees;

        private AssembledColumnFilter(@Nullable Column column,
                                      @Nullable ColumnFilter columnFilter) {
            this.column = column;
            this.columnFilter = columnFilter;
            this.subtrees = newLinkedList();
        }

        private AssembledColumnFilter() {
            this(null, null);
        }

        private Column getColumn() {
            return column;
        }

        private ColumnFilter getColumnFilter() {
            return columnFilter;
        }

        @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
            // Only non-faulty values are used.
        private Filter toFilter(ColumnHandler handler) {
            final Value<?> value = handler.toValue(column, columnFilter);
            final String columnIdentifier = columnFilter.getColumnName();
            switch (columnFilter.getOperator()) {
                case EQUAL:
                    return eq(columnIdentifier, value);
                case GREATER_THAN:
                    return gt(columnIdentifier, value);
                case LESS_THAN:
                    return lt(columnIdentifier, value);
                case GREATER_OR_EQUAL:
                    return ge(columnIdentifier, value);
                case LESS_OR_EQUAL:
                    return le(columnIdentifier, value);
                default:
                    throw new IllegalStateException(columnFilter.getOperator()
                                                                .name());
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AssembledColumnFilter that = (AssembledColumnFilter) o;
            return Objects.equal(getColumn(), that.getColumn()) &&
                    Objects.equal(getColumnFilter(), that.getColumnFilter());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getColumn(), getColumnFilter());
        }
    }

    private static final class ColumnFilterGraphHead extends AssembledColumnFilter {

        private static AssembledColumnFilter instance() {
            return new ColumnFilterGraphHead();
        }

        @Override
        public boolean equals(Object o) {
            return o == this;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    public interface ConjunctionProcessor {

        void process(Iterable<AssembledColumnFilter> conjunctiveParameter);
    }
}
