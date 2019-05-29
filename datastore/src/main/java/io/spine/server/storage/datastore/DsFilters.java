/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.Value;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMultimap;
import io.spine.client.Filter;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityColumn;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.ge;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.le;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.lt;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newLinkedList;
import static io.spine.client.CompositeFilter.CompositeOperator.ALL;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;

/**
 * A utility for working with the Datastore {@linkplain Filter filters}.
 *
 * @author Dmytro Dashenkov
 */
final class DsFilters {

    /**
     * Matches the {@link CompositeQueryParameter} instances joined by
     * the {@linkplain io.spine.client.CompositeFilter.CompositeOperator#ALL conjunctive} operator.
     */
    private static final Predicate<CompositeQueryParameter> isConjunctive =
            input -> {
                checkNotNull(input);
                return input.getOperator() == ALL;
            };

    /**
     * Matches the {@link CompositeQueryParameter} instances joined by
     * the {@linkplain io.spine.client.CompositeFilter.CompositeOperator#EITHER disjunctive}
     * operator.
     */
    private static final Predicate<CompositeQueryParameter> isDisjunctive =
            input -> !isConjunctive.test(input);

    /**
     * Prevents the utility class instantiation.
     */
    private DsFilters() {
    }

    /**
     * Converts the given {@link CompositeQueryParameter} instances into the Datastore
     * {@linkplain Filter Filters}.
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
     *
     * <p>Given query predicates {@code p1}, {@code p2}, {@code p3}, {@code p4}, {@code p5} passed
     * into the method within the following construction: {@code p1 & (p2 | p3) & (p4 | p5)}. Then
     * the resulting {@code Collection} of {@linkplain Filter Filters} will be constructed as
     * {@code (p1 & p2 & p4) | (p1 & p2 & p5) | (p1 & p3 & p4) | (p1 & p3 & p5)}.
     *
     * <p>The separate conjunctive groups (e.g. {@code (p1 & p2 & p4)}) in the result
     * {@code Collection} are placed into a single {@link Filter} instances one per group.
     *
     * <p>In other words, the predicate expression is brought into the
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
     * @param parameters
     *         the {@linkplain CompositeQueryParameter query parameters} to convert
     * @param columnFilterAdapter
     *         an instance of {@linkplain FilterAdapter} performing
     *         the required type conversions
     * @return the equivalent expression of in Datastore {@link Filter} instances
     */
    static Collection<StructuredQuery.Filter>
    fromParams(Collection<CompositeQueryParameter> parameters,
               FilterAdapter columnFilterAdapter) {
        checkNotNull(parameters);
        checkNotNull(columnFilterAdapter);

        Collection<StructuredQuery.Filter> results;
        if (parameters.isEmpty()) {
            results = emptySet();
        } else {
            results = toFilters(parameters, columnFilterAdapter);
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
    private static Collection<StructuredQuery.Filter>
    toFilters(Collection<CompositeQueryParameter> parameters,
              FilterAdapter columnFilterAdapter) {
        List<CompositeQueryParameter> conjunctionParams = parameters.stream()
                                                                    .filter(isConjunctive)
                                                                    .collect(toList());
        List<CompositeQueryParameter> disjunctionParams = parameters.stream()
                                                                    .filter(isDisjunctive)
                                                                    .collect(toList());
        Optional<CompositeQueryParameter> mergedConjunctiveParams =
                mergeConjunctiveParameters(conjunctionParams);

        Collection<StructuredQuery.Filter> filters = newLinkedList();
        TreePathWalker processor = new FilterReducer(columnFilterAdapter, filters);
        multiply(mergedConjunctiveParams.orElse(null), disjunctionParams, processor);
        return filters;
    }

    /**
     * Merges conjunctive parameters into a single {@link Filter}.
     *
     * @param conjunctiveParams
     *         list of parameters
     * @return resulting filter or {@code Optional.empty()} if there {@code conjunctiveParams} is
     *         empty
     */
    private static Optional<CompositeQueryParameter> mergeConjunctiveParameters(
            List<CompositeQueryParameter> conjunctiveParams) {
        if (!conjunctiveParams.isEmpty()) {
            CompositeQueryParameter firstParam = conjunctiveParams.get(0);
            List<CompositeQueryParameter> tailParams =
                    conjunctiveParams.subList(1, conjunctiveParams.size());
            CompositeQueryParameter mergedConjunctiveParams =
                    new ParameterReducer(tailParams).apply(firstParam);
            return Optional.of(mergedConjunctiveParams);
        } else {
            return empty();
        }
    }

    /**
     * Performs the logical multiplication of the given query predicates.
     *
     * @param constant
     *         the single non-disjunctive query parameter
     * @param parameters
     *         the disjunctive query parameters
     * @param processor
     *         the result processor
     */
    private static void multiply(@Nullable CompositeQueryParameter constant,
                                 Iterable<CompositeQueryParameter> parameters,
                                 TreePathWalker processor) {
        FilterNode expressionTree = buildConjunctionTree(constant, parameters);
        expressionTree.traverse(processor);
    }

    /**
     * Builds a tree from the given query parameter expression.
     *
     * <p>The resulting tree will start with a predefined
     * {@linkplain FilterTreeHead stub node}. The subtrees of each node contains the possible
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
     * <p>Despite the severe data duplication, which can be noticed on the schema, the tree gives
     * a handy way to {@linkplain FilterNode#traverse traverse} over.
     *
     * @param constant
     *         the single non-disjunctive query parameter
     * @param parameters
     *         the disjunctive query parameters
     * @return the top node of the build tree
     * @see #fromParams for the expression explanation
     */
    @SuppressWarnings("MethodWithMultipleLoops")
        // Complex but highly tied logic that can't be split.
    private static FilterNode buildConjunctionTree(
            @Nullable CompositeQueryParameter constant,
            Iterable<CompositeQueryParameter> parameters) {

        FilterNode lastSequentialNode;
        FilterNode head;
        if (constant != null) {
            Collection<FilterNode> filters = toFilters(constant);
            if (!filters.isEmpty()) {
                FilterNode prev = FilterTreeHead.instance();
                head = prev;
                for (FilterNode filter : filters) {
                    prev.subtrees.add(filter);
                    prev = filter;
                }
                lastSequentialNode = prev;
            } else {
                lastSequentialNode = FilterTreeHead.instance();
                head = lastSequentialNode;
            }
        } else {
            lastSequentialNode = FilterTreeHead.instance();
            head = lastSequentialNode;
        }

        Collection<FilterNode> lastLayer = singleton(lastSequentialNode);
        for (CompositeQueryParameter parameter : parameters) {
            Collection<FilterNode> currentLayer = toFilters(parameter);
            for (FilterNode thisLayerItem : currentLayer) {
                for (FilterNode prevLayerItem : lastLayer) {
                    prevLayerItem.subtrees.add(thisLayerItem);
                }
            }
            lastLayer = currentLayer;
        }
        return head;
    }

    /**
     * Converts the given parameter to a {@code Collection} of {@link FilterNode}s.
     */
    private static Collection<FilterNode> toFilters(CompositeQueryParameter param) {
        ImmutableMultimap<EntityColumn, Filter> srcFilters = param.getFilters();
        Set<FilterNode> filters = new HashSet<>(srcFilters.size());
        for (Map.Entry<EntityColumn, Filter> entry : srcFilters.entries()) {
            filters.add(new FilterNode(entry.getKey(), entry.getValue()));
        }
        return filters;
    }

    /**
     * A function performing the {@linkplain CompositeQueryParameter#conjunct conjunction} operation
     * on the {@link CompositeQueryParameter} instances.
     */
    private static class ParameterReducer
            implements Function<CompositeQueryParameter, CompositeQueryParameter> {

        private final Iterable<CompositeQueryParameter> otherParams;

        private ParameterReducer(Iterable<CompositeQueryParameter> otherParams) {
            this.otherParams = otherParams;
        }

        @Override
        public CompositeQueryParameter apply(@Nullable CompositeQueryParameter input) {
            checkNotNull(input);
            CompositeQueryParameter merged = input.conjunct(otherParams);
            return merged;
        }
    }

    /**
     * Performs sequential {@link Filter} conjunction.
     *
     * <p>The walker transforms all the {@link FilterNode} instances into {@link Filter}
     * instances and merges them using
     * the {@link CompositeFilter#and
     * StructuredQuery.CompositeFilter.and()} operation. The result is then collected into
     * {@code Collection} passed on the instance creation.
     */
    private static class FilterReducer implements TreePathWalker {

        private final FilterAdapter columnFilterAdapter;
        private final Collection<StructuredQuery.Filter> destination;

        private FilterReducer(FilterAdapter columnFilterAdapter,
                              Collection<StructuredQuery.Filter> destination) {
            this.columnFilterAdapter = columnFilterAdapter;
            this.destination = destination;
        }

        @SuppressWarnings("ZeroLengthArrayAllocation")
            // It is used to create a typed array from the collection.
        @Override
        public void walk(Collection<FilterNode> conjunctionGroup) {
            if (conjunctionGroup.isEmpty()) {
                return;
            }
            Function<FilterNode, StructuredQuery.Filter> mapper =
                    FilterNode.toFilterFunction(columnFilterAdapter);
            List<StructuredQuery.Filter> filters = conjunctionGroup.stream()
                                                                   .map(mapper)
                                                                   .collect(toList());
            checkState(!filters.isEmpty());
            StructuredQuery.Filter first = filters.get(0);
            StructuredQuery.Filter[] other = filters.subList(1, filters.size())
                                                    .toArray(new StructuredQuery.Filter[0]);
            StructuredQuery.Filter group = and(first, other);
            destination.add(group);
        }
    }

    /**
     * A tree-structure specific representation of the {@link Filter}.
     *
     * <p>The type holds the information about the {@link Filter} and a {@code Collection} of
     * references on the subtrees of given node.
     */
    private static class FilterNode {

        private final EntityColumn column;
        private final Filter columnFilter;

        private final Collection<FilterNode> subtrees;

        private FilterNode(@Nullable EntityColumn column, @Nullable Filter filter) {
            this.column = column;
            this.columnFilter = filter;
            this.subtrees = newLinkedList();
        }

        private FilterNode() {
            this(null, null);
        }

        private EntityColumn getColumn() {
            return column;
        }

        private Filter getFilter() {
            return columnFilter;
        }

        @SuppressWarnings({"EnumSwitchStatementWhichMissesCases", "MethodOnlyUsedFromInnerClass"})
            // Only non-faulty values are used.
        private StructuredQuery.Filter toFilter(FilterAdapter adapter) {
            Value<?> value = adapter.toValue(column, columnFilter);
            String columnIdentifier = column.getStoredName();
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
         * Traverses the tree of the {@code FilterNode column filters} starting from
         * the current node, passing all the found paths from the tree top to a leaf to the given
         * result processor.
         *
         * @param processor
         *         the result processor
         */
        @SuppressWarnings("MethodWithMultipleLoops")
            // To make the traversal algorithm more obvious.
        private void traverse(TreePathWalker processor) {
            Queue<FilterNode> nodes = new ArrayDeque<>();
            Queue<Collection<FilterNode>> paths = new ArrayDeque<>();
            nodes.offer(this);
            // Initial path is intentionally left empty.
            paths.offer(new ArrayList<>());

            while (!nodes.isEmpty()) {
                FilterNode node = nodes.poll();
                Collection<FilterNode> path = paths.poll();

                checkNotNull(path);
                if (node.isLeaf()) {
                    processor.walk(path);
                } else {
                    for (FilterNode child : node.subtrees) {
                        Collection<FilterNode> childPath = newPath(path);
                        childPath.add(child);
                        nodes.offer(child);
                        paths.offer(childPath);
                    }
                }
            }
        }

        private static <T> Collection<T> newPath(Collection<T> oldOne) {
            return new ArrayList<>(oldOne);
        }

        private boolean isLeaf() {
            return subtrees.isEmpty();
        }

        /**
         * {@inheritDoc}
         *
         * <p>An arbitrary node is never equal to the tree head. The tree head is only equal to
         * itself.
         */
        @SuppressWarnings("EqualsGetClass")
            // This is a private class as well as the descendant overriding equals().
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilterNode that = (FilterNode) o;
            return Objects.equal(getColumn(), that.getColumn()) &&
                    Objects.equal(getFilter(), that.getFilter());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getColumn(), getFilter());
        }

        private static Function<FilterNode, StructuredQuery.Filter>
        toFilterFunction(FilterAdapter adapter) {
            return new ToFilter(adapter);
        }

        private static class ToFilter implements Function<FilterNode, StructuredQuery.Filter> {

            private final FilterAdapter adapter;

            private ToFilter(FilterAdapter adapter) {
                this.adapter = adapter;
            }

            @Override
            public StructuredQuery.Filter apply(@Nullable FilterNode input) {
                checkNotNull(input);
                StructuredQuery.Filter result = input.toFilter(adapter);
                return result;
            }
        }
    }

    /**
     * A specific type of a {@code FilterNode} tree node representing the tree head.
     */
    private static final class FilterTreeHead extends FilterNode {

        private static FilterNode instance() {
            return new FilterTreeHead();
        }

        /**
         * {@inheritDoc}
         *
         * <p>A tree head is only equal to itself.
         *
         * @implNote Uses reference equality.
         */
        @Override
        public boolean equals(Object o) {
            return o == this;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    /**
     * A functional interface defining the operation of precessing a conjunction group found by
     * the {@linkplain #fromParams parenthesis simplifying algorithm}.
     *
     * <p>Receives a {@code Collection} of {@linkplain FilterNode FilterNodes}
     * representing a single generated conjunction group.
     *
     * @see FilterReducer for the implementation
     */
    @FunctionalInterface
    private interface TreePathWalker {

        /**
         * Walks a path in {@linkplain #buildConjunctionTree the conjunction tree} and performs
         * a specified action for the given path.
         *
         * <p>The given path represents a single conjunction group.
         *
         * @param conjunctionGroup
         *         the path in
         *         the {@linkplain #buildConjunctionTree conjunction tree}
         *         representing a single conjunction group
         */
        void walk(Collection<FilterNode> conjunctionGroup);
    }
}
