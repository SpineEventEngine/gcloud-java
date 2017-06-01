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

import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.cloud.datastore.Value;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.spine3.client.ColumnFilter;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.CompositeQueryParameter;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
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
 * A utility for working with the Datastore {@linkplain Filter filters}.
 *
 * @author Dmytro Dashenkov
 */
final class DsFilters {

    /**
     * Matches the {@link CompositeQueryParameter} instances joined by
     * the {@linkplain org.spine3.client.CompositeColumnFilter.CompositeOperator#ALL conjunctive}
     * operator.
     */
    private static final Predicate<CompositeQueryParameter> isConjunctive =
            new Predicate<CompositeQueryParameter>() {
                @Override
                public boolean apply(@Nullable CompositeQueryParameter input) {
                    return input != null && input.getOperator() == ALL;
                }
            };

    /**
     * Matches the {@link CompositeQueryParameter} instances joined by
     * the {@linkplain org.spine3.client.CompositeColumnFilter.CompositeOperator#EITHER disjunctive}
     * operator.
     */
    private static final Predicate<CompositeQueryParameter> isDisjunctive = not(isConjunctive);

    private DsFilters() {
        // Prevent utility class fromm being initialized.
    }

    /**
     * Converts the given {@link CompositeQueryParameter} instances into the Datastore
     * {@link Filter}s.
     *
     * <p>The returned {@code Collection} contains the same predicate as the passed
     * {@code Collection} of query parameters. The difference is in
     * <ul>
     *     <li>type: from Spine {@link CompositeQueryParameter} to Datastore {@link Filter};
     *     <li>format: all the resulting {@link Filter}s are conjunctive (as Datastore does not
     *         support disjunction) and are related with each other in the disjunctive way.
     * </ul>
     *
     * <p><i>Example:</i>
     * <p>Given query predicates {@code p1}, {@code p2}, {@code p3}, {@code p4}, {@code p5}. These
     * predicates are passed into the method in the following construction:
     * {@code p1 & (p2 | p3) & (p4 | p5)}. Then the resulting {@code Collection} of {@link Filter}s
     * will be constructed as
     * {@code (p1 & p2 & p4) | (p1 & p2 & p5) | (p1 & p3 & p4) | (p1 & p3 & p5)}, where the separate
     * conjunctive groups are placed into a single {@link Filter} instance per one group. In other
     * words, the predicate expression is brought into the
     * <a href="https://en.wikipedia.org/wiki/Disjunctive_normal_form">disjunctive normal form</a>.
     *
     * <p>Note that by the convention, the separate
     * {@link CompositeQueryParameter} instances are considered to be joined by the {@code &}
     * operator. but the resulting {@link Filter} instances are specified to be joined with
     * {@code |} operator, so that the merged result of executing the queries with each of
     * the filters will be the result of the whole expression.
     *
     * <p>If the given parameter {@code Collection} is empty, and empty {@code Collection}
     * is returned.
     *
     * @param parameters    the {@linkplain CompositeQueryParameter query parameters} to convert
     * @param columnHandler an instance of {@linkplain ColumnHandler} performing the required type
     *                      conventions
     * @return the equivalent expression of in Datastore {@link Filter} instances
     */
    static Collection<Filter> fromParams(Collection<CompositeQueryParameter> parameters,
                                         ColumnHandler columnHandler) {
        checkNotNull(parameters);
        checkNotNull(columnHandler);

        final Collection<Filter> results;
        if (parameters.isEmpty()) {
            results = emptySet();
        } else {
            results = toFilters(parameters, columnHandler);
        }
        return results;
    }

    /**
     * Performs the actual expression conversion.
     *
     * <p>This method divides the given params into conjunctive and disjunctive. The conjunctive
     * parameters are then merged into a single {@link Filter}. After that, the disjunction
     * parentheses are opened and the {@linkplain #multiply logical multiplication} is performed.
     */
    private static Collection<Filter> toFilters(Collection<CompositeQueryParameter> parameters,
                                                final ColumnHandler columnHandler) {
        final FluentIterable<CompositeQueryParameter> params = from(parameters);
        final FluentIterable<CompositeQueryParameter> conjunctionParams =
                params.filter(isConjunctive);
        final FluentIterable<CompositeQueryParameter> disjunctionParams =
                params.filter(isDisjunctive);
        final Optional<CompositeQueryParameter> firstParam = conjunctionParams.first();
        final Optional<CompositeQueryParameter> mergedConjunctiveParams =
                firstParam.transform(new ParameterShrinker(conjunctionParams.skip(1)));
        final Collection<Filter> filters = newLinkedList();
        final ConjunctionProcessor processor = new ColumnFilterShrinker(columnHandler, filters);
        multiply(mergedConjunctiveParams.orNull(), disjunctionParams, processor);
        return filters;
    }

    /**
     * Performs the logical multiplication of the given query predicates.
     *
     * @param constant   the single non-disjunctive query parameter
     * @param parameters the disjunctive query parameters
     * @param processor  the result processor
     */
    private static void multiply(@Nullable CompositeQueryParameter constant,
                                 Iterable<CompositeQueryParameter> parameters,
                                 ConjunctionProcessor processor) {
        final ColumnFilterNode expressionTree = buildConjunctionTree(constant,
                                                                     parameters);
        traverse(expressionTree, Lists.<ColumnFilterNode>newLinkedList(), processor);
    }

    private static <T> Queue<T> newPath(Collection<T> oldOne) {
        return new LinkedList<>(oldOne);
    }

    /**
     * Traverses the tree of the {@linkplain ColumnFilterNode column filters} recursively,
     * passing all the found paths from the tree top to a leaf to the given result processor.
     *
     * @param node      the tree node to start the traversal from
     * @param prefix    the path to the current node from the top
     * @param processor the result processor
     */
    private static void traverse(ColumnFilterNode node,
                                 Queue<ColumnFilterNode> prefix,
                                 ConjunctionProcessor processor) {
        node.appendTo(prefix);
        if (node.subtrees.isEmpty()) {
            processor.process(prefix);
        } else {
            for (ColumnFilterNode successor : node.subtrees) {
                traverse(successor, newPath(prefix), processor);
            }
        }
    }

    /**
     * Builds a tree from the given query parameter expression.
     *
     * <p>The resulting tree will start with a predefined
     * {@linkplain ColumnFilterTreeHead stub node}. The subtrees of each node contains the possible
     * multipliers of the filter represented by this node.
     *
     * <p>The tree structure is so, that each path from the top to each leaf is a disjunctive group
     * in the disjunctive normal from which represents the given expression.
     *
     * <p>For the expression in the example ({@code p1 & (p2 | p3) & (p4 | p5)} - see
     * {@link #fromParams fromParams}) the tree would be:
     * <pre>
     *
     *           HEAD
     *            |
     *           (p1)
     *          /   \
     *         /     \
     *     (p2)      (p3)
     *    /   \     /   \
     * (p4)  (p5) (p4)  (p5)
     *
     * </pre>
     *
     * <p>Despite the severe data duplication which can be noticed on the schema, the tree gives
     * a handy way to {@linkplain #traverse traverse} over.
     *
     * @param constant   the single non-disjunctive query parameter
     * @param parameters the disjunctive query parameters
     * @return the top node of the build tree
     * @see #fromParams for the expression explanation
     */
    @SuppressWarnings("MethodWithMultipleLoops")
        // Complex but highly tied logic that can't be split.
    private static ColumnFilterNode buildConjunctionTree(
            @Nullable CompositeQueryParameter constant,
            Iterable<CompositeQueryParameter> parameters) {

        final ColumnFilterNode lastSequentialNode;
        final ColumnFilterNode head;
        if (constant != null) {
            final Collection<ColumnFilterNode> filters = toFilters(constant);
            if (!filters.isEmpty()) {
                ColumnFilterNode prev = ColumnFilterTreeHead.instance();
                head = prev;
                for (ColumnFilterNode filter : filters) {
                    prev.subtrees.add(filter);
                    prev = filter;
                }
                lastSequentialNode = prev;
            } else {
                lastSequentialNode = ColumnFilterTreeHead.instance();
                head = lastSequentialNode;
            }
        } else {
            lastSequentialNode = ColumnFilterTreeHead.instance();
            head = lastSequentialNode;
        }

        Collection<ColumnFilterNode> lastLayer = singleton(lastSequentialNode);
        for (CompositeQueryParameter parameter : parameters) {
            final Collection<ColumnFilterNode> currentLayer = toFilters(parameter);
            for (ColumnFilterNode thisLayerItem : currentLayer) {
                for (ColumnFilterNode prevLayerItem : lastLayer) {
                    prevLayerItem.subtrees.add(thisLayerItem);
                }
            }
            lastLayer = currentLayer;
        }
        return head;
    }

    /**
     * Converts the given parameter to a {@code Collection} of {@link ColumnFilterNode}s.
     */
    private static Collection<ColumnFilterNode> toFilters(CompositeQueryParameter param) {
        final Multimap<Column, ColumnFilter> srcFilters = param.getFilters();
        final Set<ColumnFilterNode> filters = new HashSet<>(srcFilters.size());
        for (Map.Entry<Column, ColumnFilter> entry : srcFilters.entries()) {
            filters.add(new ColumnFilterNode(entry.getKey(), entry.getValue()));
        }
        return filters;
    }

    /**
     * A function performing the {@linkplain CompositeQueryParameter#conjunct conjunction} operation
     * on the {@link CompositeQueryParameter} instances.
     */
    private static class ParameterShrinker
            implements Function<CompositeQueryParameter, CompositeQueryParameter> {

        private final Iterable<CompositeQueryParameter> otherParams;

        private ParameterShrinker(Iterable<CompositeQueryParameter> otherParams) {
            this.otherParams = otherParams;
        }

        @Override
        public CompositeQueryParameter apply(@Nullable CompositeQueryParameter input) {
            checkNotNull(input);
            final CompositeQueryParameter merged = input.conjunct(otherParams);
            return merged;
        }
    }

    /**
     * An implementation of {@link ConjunctionProcessor} performing sequential {@link Filter}
     * conjunction.
     *
     * <p>The processor polls all the {@link ColumnFilterNode} instances, transforms them into
     * {@link Filter} instances and merges with
     * the {@link com.google.cloud.datastore.StructuredQuery.CompositeFilter#and
     * StructuredQuery.CompositeFilter.and()} operation. The result is then collected into
     * {@code Collection} passed on the instance creation.
     *
     * <p>The {@link Queue} passed to the function becomes empty after the processing.
     */
    private static class ColumnFilterShrinker implements ConjunctionProcessor {

        private final ColumnHandler columnHandler;
        private final Collection<Filter> filters;

        private ColumnFilterShrinker(ColumnHandler columnHandler,
                                     Collection<Filter> filters) {
            this.columnHandler = columnHandler;
            this.filters = filters;
        }

        @Override
        public void process(Queue<ColumnFilterNode> columnFilters) {
            Filter filter;
            if (!columnFilters.isEmpty()) {
                filter = columnFilters.poll()
                                      .toFilter(columnHandler);
            } else {
                return;
            }
            while (!columnFilters.isEmpty()) {
                final Filter propFilter = columnFilters.poll()
                                                       .toFilter(columnHandler);
                filter = and(filter, propFilter);
            }
            filters.add(filter);
        }
    }

    /**
     * A tree-structure specific representation of the {@link ColumnFilter}.
     *
     * <p>The type holds the information about the {@link ColumnFilter} and a {@code Collection} of
     * references on the subtrees of given node.
     */
    private static class ColumnFilterNode {

        private final Column column;
        private final ColumnFilter columnFilter;

        private final Collection<ColumnFilterNode> subtrees;

        private ColumnFilterNode(@Nullable Column column,
                                 @Nullable ColumnFilter columnFilter) {
            this.column = column;
            this.columnFilter = columnFilter;
            this.subtrees = newLinkedList();
        }

        private ColumnFilterNode() {
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

        /**
         * Appends this instance to the given path in the tree.
         *
         * <p>The path is represented by a {@link Queue} of nodes.
         *
         * <p>In th general case, the method just enqueues current node into the given path.
         */
        void appendTo(Queue<ColumnFilterNode> path) {
            path.offer(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnFilterNode that = (ColumnFilterNode) o;
            return Objects.equal(getColumn(), that.getColumn()) &&
                    Objects.equal(getColumnFilter(), that.getColumnFilter());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getColumn(), getColumnFilter());
        }
    }

    /**
     * A specific type of a {@code ColumnFilterNode} tree node representing the tree head.
     */
    private static final class ColumnFilterTreeHead extends ColumnFilterNode {

        private static ColumnFilterNode instance() {
            return new ColumnFilterTreeHead();
        }

        @Override
        public boolean equals(Object o) {
            return o == this;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        /**
         * {@inheritDoc}
         *
         * <p>This implementation performs no action so that no instances of
         * {@link ColumnFilterTreeHead} is found in a path.
         */
        @Override
        void appendTo(Queue<ColumnFilterNode> path) {
            // NoOp
        }
    }

    /**
     * A functional interface defining the operation of precessing a conjunction group found by
     * the {@linkplain #fromParams parenthesis opening algorithm}.
     */
    private interface ConjunctionProcessor {

        /**
         * Processes the found conjunction.
         */
        void process(Queue<ColumnFilterNode> conjunctiveParameter);
    }
}
