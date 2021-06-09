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

import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.spine.client.Filter;
import io.spine.query.QueryPredicate;
import io.spine.query.SubjectParameter;
import io.spine.server.storage.ColumnMapping;
import io.spine.server.storage.ColumnTypeMapping;

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
     * Converts the given {@link CompositeQueryParameter} instances into the Datastore
     * {@link Filter Filters}.
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
//    static Collection<StructuredQuery.Filter>
//    fromParams(Collection<CompositeQueryParameter> parameters,
//               FilterAdapter columnFilterAdapter) {
//        checkNotNull(parameters);
//        checkNotNull(columnFilterAdapter);
//
//        Collection<StructuredQuery.Filter> results;
//        if (parameters.isEmpty()) {
//            results = emptySet();
//        } else {
//            results = toFilters(parameters, columnFilterAdapter);
//        }
//        return results;
//    }

    /**
     * Converts the given {@link QueryPredicate} instances into the Datastore
     * {@link Filter Filters}.
     *
     * <p>The returned {@code Collection} contains the same predicate as the passed
     * {@code Collection} of query parameters. The difference is in
     * <ul>
     *     <li>type: from a {@code QueryPredicate} to Datastore {@link Filter};
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
     * <p>Note that by the convention, the distinct
     * {@link QueryPredicate} instances are considered to be joined by the {@code &}
     * operator. but the resulting {@link Filter} instances are specified to be joined with
     * {@code |} operator, so that the merged result of executing the queries with each of
     * the filters will be the result of the whole expression.
     *
     * <p>If the given parameter {@code Collection} is empty, and empty {@code Collection}
     * is returned.
     *
     * @param predicate
     *         the predicate to convert
     * @param adapter
     *         an adapter performing the required type conversions
     * @return the equivalent expression of in Datastore {@link Filter} instances
     */
    static <R extends Message> Collection<StructuredQuery.Filter>
    fromPredicate(QueryPredicate<R> predicate, FilterAdapter adapter) {
        checkNotNull(predicate);
        checkNotNull(adapter);

        Collection<StructuredQuery.Filter> results;
        if(predicate.isEmpty()) {
            results = emptySet();
        } else {
            results = toDsFilters(predicate, adapter);
        }
        return results;
    }

    private static <R extends Message> Collection<StructuredQuery.Filter>
    toDsFilters(QueryPredicate<R> predicate, FilterAdapter adapter) {
        ImmutableList.Builder<StructuredQuery.Filter> result = ImmutableList.builder();
        QueryPredicate<R> dnf = predicate.toDnf();
        ColumnMapping<Value<?>> mapping = adapter.columnMapping();
        if (dnf.operator() == AND) {
            StructuredQuery.Filter group = handleConjunctiveGroup(dnf, mapping);
            result.add(group);
        } else {
            checkState(dnf.allParams()
                          .isEmpty(),
                       "Top-level disjunctive predicate in DNF " +
                               "must not have its own parameters.");
            ImmutableList<QueryPredicate<R>> children = dnf.children();
            for (QueryPredicate<R> child : children) {
                StructuredQuery.Filter group = handleConjunctiveGroup(child, mapping);
                result.add(group);
            }
        }
        return result.build();

//
//        Optional<QueryPredicate<R>> rootConjunctivePredicate = Optional.empty();
//        if(predicate.operator() == AND && !predicate.allParams().isEmpty()) {
//            rootConjunctivePredicate = Optional.of(predicate);
//        }
//
//        ImmutableList<QueryPredicate<R>> childPredicates = predicate.children();
//        if(!childPredicates.isEmpty()) {
//
//        }
//
//        Collection<StructuredQuery.Filter> filters = newLinkedList();
//        TreePathWalker<R> processor = new FilterReducer<>(adapter, filters);
//        multiply(rootConjunctivePredicate.orElse(null), disjunctionParams, processor);
//
//
//        ///
//        Map<Boolean, List<QueryPredicate<R>>> partitions =
//                predicate.stream()
//                          .collect(partitioningBy(DsFilters::isConjunctive));
//        List<QueryPredicate<R>> conjunctionParams = partitions.get(true);
//        List<QueryPredicate<R>> disjunctionParams = partitions.get(false);
//        Optional<QueryPredicate<R>> mergedConjunctiveParams =
//                mergeConjunctiveParameters(conjunctionParams);
//
//        Collection<StructuredQuery.Filter> filters = newLinkedList();
//        TreePathWalker<R> processor = new FilterReducer<>(adapter, filters);
//        multiply(mergedConjunctiveParams.orElse(null), disjunctionParams, processor);
//        return filters;
    }

    private static <R extends Message> StructuredQuery.Filter
    handleConjunctiveGroup(QueryPredicate<R> predicate, ColumnMapping<Value<?>> mapping) {

        checkState(predicate.children().isEmpty(),
                   "Children collection must be empty for a conjunctive predicate group.");
        ImmutableList<SubjectParameter<?, ?, ?>> parameters = predicate.allParams();
        List<StructuredQuery.Filter> filters =
                parameters.stream()
                          .map(param -> createFilter(param, mapping))
                          .collect(toList());

        checkState(!filters.isEmpty());
        StructuredQuery.Filter first = filters.get(0);
        StructuredQuery.Filter[] other = filters.subList(1, filters.size())
                                                .toArray(new StructuredQuery.Filter[filters.size() - 1]);
        StructuredQuery.Filter group = and(first, other);
        return group;
    }

    private static StructuredQuery.PropertyFilter
    createFilter(SubjectParameter<?, ?, ?> parameter, ColumnMapping<Value<?>> mapping) {
        checkNotNull(parameter);
        checkNotNull(mapping);
        Object paramValue = parameter.value();
        ColumnTypeMapping<?, ? extends Value<?>> typeMapping = mapping.of(paramValue.getClass());
        Value<?> value = typeMapping.applyTo(paramValue);
        String columnName = parameter.column()
                                     .name()
                                     .value();
        switch (parameter.operator()) {
            case EQUALS:
                return eq(columnName, value);
            case GREATER_THAN:
                return gt(columnName, value);
            case LESS_THAN:
                return lt(columnName, value);
            case GREATER_OR_EQUALS:
                return ge(columnName, value);
            case LESS_OR_EQUALS:
                return le(columnName, value);
            default:
                throw new IllegalStateException(parameter.operator().name());
        }
    }

//    /**
//     * Performs the actual expression conversion.
//     *
//     * <p>This method divides the given params into conjunctive and disjunctive. The conjunctive
//     * parameters are then merged into a single {@link Filter}. After that, the disjunction
//     * parentheses are opened and the {@linkplain #multiply logical multiplication} is performed.
//     */
//    private static Collection<StructuredQuery.Filter>
//    toFilters(Collection<CompositeQueryParameter> parameters,
//              FilterAdapter columnFilterAdapter) {
//
//        Map<Boolean, List<CompositeQueryParameter>> partitions =
//                parameters.stream()
//                          .collect(partitioningBy(DsFilters::isConjunctive));
//        List<CompositeQueryParameter> conjunctionParams = partitions.get(true);
//        List<CompositeQueryParameter> disjunctionParams = partitions.get(false);
//        Optional<CompositeQueryParameter> mergedConjunctiveParams =
//                mergeConjunctiveParameters(conjunctionParams);
//
//        Collection<StructuredQuery.Filter> filters = newLinkedList();
//        TreePathWalker processor = new FilterReducer(columnFilterAdapter, filters);
//        multiply(mergedConjunctiveParams.orElse(null), disjunctionParams, processor);
//        return filters;
//    }

//    /**
//     * Matches the {@link CompositeQueryParameter} instances joined by
//     * the {@linkplain io.spine.client.CompositeFilter.CompositeOperator#ALL conjunctive} operator.
//     */
//    private static boolean isConjunctive(CompositeQueryParameter param) {
//        checkNotNull(param);
//        return param.operator() == ALL;
//    }

//    /**
//     * Matches the {@link QueryPredicate} instances joined by
//     * the {@linkplain LogicalOperator#AND conjunctive} operator.
//     */
//    private static <R extends Message> boolean isConjunctive(QueryPredicate<R> predicate) {
//        checkNotNull(predicate);
//        return predicate.operator() == AND;
//    }
//
//    /**
//     * Merges conjunctive parameters into a single {@link Filter}.
//     *
//     * @param predicates
//     *         list of parameters
//     * @return resulting filter or {@code Optional.empty()} if there {@code conjunctiveParams} is
//     *         empty
//     */
//    private static <R extends Message>
//    Optional<QueryPredicate<R>> mergeConjunctiveParameters(List<QueryPredicate<R>> predicates) {
//        if (!predicates.isEmpty()) {
//            QueryPredicate<R> firstParam = predicates.get(0);
//            List<QueryPredicate<R>> tailParams = predicates.subList(1, predicates.size());
//            QueryPredicate<R> mergedConjunctiveParams =
//                    new PredicateReducer<>(tailParams).apply(firstParam);
//            return Optional.of(mergedConjunctiveParams);
//        } else {
//            return empty();
//        }
//    }

//    /**
//     * Performs the logical multiplication of the given query predicates.
//     *
//     * @param constant
//     *         the single non-disjunctive query parameter
//     * @param parameters
//     *         the disjunctive query parameters
//     * @param processor
//     *         the result processor
//     */
//    private static <R extends Message>
//    void multiply(@Nullable QueryPredicate<R> constant,
//                  Iterable<QueryPredicate<R>> parameters,
//                  TreePathWalker<R> processor) {
//        FilterNode<R> expressionTree = conjunctionTree(constant, parameters);
//        expressionTree.traverse(processor);
//    }

//    /**
//     * Builds a tree from the given query parameter expression.
//     *
//     * <p>The resulting tree will start with a predefined
//     * {@linkplain FilterTreeHead stub node}. The subtrees of each node contains the possible
//     * multipliers of the filter represented by this node.
//     *
//     * <p>The tree structure is so, that each path from the top to each leaf is a disjunctive group
//     * in the disjunctive normal from which represents the given expression.
//     *
//     * <p>For the expression in the example ({@code p1 & (p2 | p3) & (p4 | p5)} - see
//     * {@link #fromPredicate fromPredicate}) the tree would be:
//     * <pre>
//     *
//     *           HEAD
//     *            |
//     *           (p1)
//     *          /   \
//     *         /     \
//     *     (p2)      (p3)
//     *    /   \     /   \
//     * (p4)  (p5) (p4)  (p5)
//     *
//     * </pre>
//     *
//     * <p>Which gives an equivalent expression in DNF:
//     *
//     *      {@code (p1 & p2 & p4) | (p1 & p2 & p5) | (p1 & p3 & p4) | (p1 & p3 & p5)}
//     *
//     * <p>Despite the severe data duplication, which can be noticed on the schema, the tree gives
//     * a handy way to {@linkplain FilterNode#traverse traverse} over.
//     *
//     * @param constant
//     *         the single non-disjunctive query parameter
//     * @param predicates
//     *         the disjunctive query parameters
//     * @return the top node of the build tree
//     * @see #fromPredicate for the expression explanation
//     */
//    @SuppressWarnings("MethodWithMultipleLoops")  /* Coupled, complex logic that can't be split. */
//    private static <R extends Message> FilterNode<R>
//    conjunctionTree(@Nullable QueryPredicate<R> constant, Iterable<QueryPredicate<R>> predicates) {
//
//        FilterNode<R> lastSequentialNode;
//        FilterNode<R> head;
//        if (constant != null) {
//            Collection<FilterNode<R>> filters = toFilters(constant);
//            if (!filters.isEmpty()) {
//                FilterNode<R> prev = FilterTreeHead.instance();
//                head = prev;
//                for (FilterNode<R> filter : filters) {
//                    prev.subtrees.add(filter);
//                    prev = filter;
//                }
//                lastSequentialNode = prev;
//            } else {
//                lastSequentialNode = FilterTreeHead.instance();
//                head = lastSequentialNode;
//            }
//        } else {
//            lastSequentialNode = FilterTreeHead.instance();
//            head = lastSequentialNode;
//        }
//
//        Collection<FilterNode<R>> lastLayer = singleton(lastSequentialNode);
//        for (QueryPredicate<R> predicate : predicates) {
//            Collection<FilterNode<R>> currentLayer = toFilters(predicate);
//            for (FilterNode<R> thisLayerItem : currentLayer) {
//                for (FilterNode<R> prevLayerItem : lastLayer) {
//                    prevLayerItem.subtrees.add(thisLayerItem);
//                }
//            }
//            lastLayer = currentLayer;
//        }
//        return head;
//    }


//    /**
//     * Converts the given parameter to a {@code Collection} of {@link FilterNode}s.
//     */
//    private static <R extends Message>
//    Collection<FilterNode<R>> toFilters(QueryPredicate<R> predicate) {
//        ImmutableList<SubjectParameter<R, ?, ?>> srcParams = predicate.parameters();
//        Set<FilterNode<R>> filters = new HashSet<>(srcParams.size());
//        for (SubjectParameter<R, ?, ?> srcParam : srcParams) {
//            filters.add(new FilterNode<>(srcParam));
//        }
//        return filters;
//    }

//    /**
//     * A function performing the {@linkplain LogicalOperator#OR conjunction} operation
//     * on the {@link QueryPredicate} instances.
//     */
//    private static class PredicateReducer<R extends Message>
//            implements Function<QueryPredicate<R>, QueryPredicate<R>> {
//
//        private final Iterable<QueryPredicate<R>> otherPredicates;
//
//        private PredicateReducer(Iterable<QueryPredicate<R>> otherPredicates) {
//            this.otherPredicates = otherPredicates;
//        }
//
//        @Override
//        public QueryPredicate<R> apply(@Nullable QueryPredicate<R> input) {
//            checkNotNull(input);
//            ImmutableList<QueryPredicate<R>> allPredicates =
//                    ImmutableList.<QueryPredicate<R>>builder()
//                            .add(input)
//                            .addAll(otherPredicates)
//                            .build();
//            QueryPredicate<R> merged =
//                    QueryPredicate.merge(allPredicates, input.operator());
//            return merged;
//        }
//    }

//    /**
//     * Performs sequential {@link Filter} conjunction.
//     *
//     * <p>The walker transforms all the {@link FilterNode} instances into {@link Filter}
//     * instances and merges them using
//     * the {@link CompositeFilter#and
//     * StructuredQuery.CompositeFilter.and()} operation. The result is then collected into
//     * {@code Collection} passed on the instance creation.
//     */
//    private static class FilterReducer<R extends Message> implements TreePathWalker<R> {
//
//        private final FilterAdapter columnFilterAdapter;
//        private final Collection<StructuredQuery.Filter> destination;
//
//        private FilterReducer(FilterAdapter columnFilterAdapter,
//                              Collection<StructuredQuery.Filter> destination) {
//            this.columnFilterAdapter = columnFilterAdapter;
//            this.destination = destination;
//        }
//
//        @SuppressWarnings("ZeroLengthArrayAllocation")  /* Converting a collection to an array.*/
//        @Override
//        public void walk(Collection<FilterNode<R>> conjunctionGroup) {
//            if (conjunctionGroup.isEmpty()) {
//                return;
//            }
//            Function<FilterNode<R>, StructuredQuery.Filter> mapper =
//                    FilterNode.toFilterFunction(columnFilterAdapter);
//            List<StructuredQuery.Filter> filters = conjunctionGroup.stream()
//                                                                   .map(mapper)
//                                                                   .collect(toList());
//            checkState(!filters.isEmpty());
//            StructuredQuery.Filter first = filters.get(0);
//            StructuredQuery.Filter[] other = filters.subList(1, filters.size())
//                                                    .toArray(new StructuredQuery.Filter[0]);
//            StructuredQuery.Filter group = and(first, other);
//            destination.add(group);
//        }
//    }

//    /**
//     * A tree-structure specific representation of the {@link Filter}.
//     *
//     * <p>The type holds the information about the {@link Filter} and a {@code Collection} of
//     * references on the subtrees of given node.
//     */
//    private static class FilterNode<R extends Message> {
//
//        /**
//         * A parameter putting some restrictions onto the queried subject.
//         *
//         * <p>Remains {@code null} in case this filter node is a tree head.
//         */
//        private final @Nullable SubjectParameter<R, ?, ?> parameterValue;
//
//        private final Collection<FilterNode<R>> subtrees;
//
//        private FilterNode(@Nullable SubjectParameter<R, ?, ?> parameter) {
//            this.parameterValue = parameter;
//            this.subtrees = newLinkedList();
//        }
//
//        private FilterNode() {
//            this(null);
//        }
//
//        private SubjectParameter<R, ?, ?> parameter() {
//            return checkNotNull(parameterValue);
//        }
//
////        private Filter filter() {
////            return columnFilter;
////        }
//
//        @SuppressWarnings("MethodOnlyUsedFromInnerClass")   /* Only non-faulty values are used. */
//        private StructuredQuery.Filter toFilter(FilterAdapter adapter) {
//            SubjectParameter<R, ?, ?> parameter = parameter();
//            return createFilter(adapter, parameter);
//
////            Value<?> value = adapter.toValue(column, columnFilter);
////            String columnName = column.name()
////                                      .value();
////            switch (columnFilter.getOperator()) {
////                case EQUAL:
////                    return eq(columnName, value);
////                case GREATER_THAN:
////                    return gt(columnName, value);
////                case LESS_THAN:
////                    return lt(columnName, value);
////                case GREATER_OR_EQUAL:
////                    return ge(columnName, value);
////                case LESS_OR_EQUAL:
////                    return le(columnName, value);
////                default:
////                    throw new IllegalStateException(columnFilter.getOperator()
////                                                                .name());
////            }
//        }
//
////        /**
////         * Traverses the tree of the {@code FilterNode column filters} starting from
////         * the current node, passing all the found paths from the tree top to a leaf to the given
////         * result processor.
////         *
////         * @param processor
////         *         the result processor
////         */
////        @SuppressWarnings("MethodWithMultipleLoops")
////        // To make the traversal algorithm more obvious.
////        private void traverse(TreePathWalker<R> processor) {
////            Queue<FilterNode<R>> nodes = new ArrayDeque<>();
////            Queue<Collection<FilterNode<R>>> paths = new ArrayDeque<>();
////            nodes.offer(this);
////            // Initial path is intentionally left empty.
////            paths.offer(new ArrayList<>());
////
////            while (!nodes.isEmpty()) {
////                FilterNode<R> node = nodes.poll();
////                Collection<FilterNode<R>> path = paths.poll();
////
////                checkNotNull(path);
////                if (node.isLeaf()) {
////                    processor.walk(path);
////                } else {
////                    for (FilterNode<R> child : node.subtrees) {
////                        Collection<FilterNode<R>> childPath = newPath(path);
////                        childPath.add(child);
////                        nodes.offer(child);
////                        paths.offer(childPath);
////                    }
////                }
////            }
////        }
//
//        private static <T> Collection<T> newPath(Collection<T> oldOne) {
//            return new ArrayList<>(oldOne);
//        }
//
//        private boolean isLeaf() {
//            return subtrees.isEmpty();
//        }
//
//        /**
//         * {@inheritDoc}
//         *
//         * <p>An arbitrary node is never equal to the tree head. The tree head is only equal to
//         * itself.
//         */
//        @SuppressWarnings("EqualsGetClass")
//        // This is a private class as well as the descendant overriding equals().
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) {
//                return true;
//            }
//            if (o == null || getClass() != o.getClass()) {
//                return false;
//            }
//            @SuppressWarnings("unchecked")
//            FilterNode<R> that = (FilterNode<R>) o;
//            return Objects.equal(parameter(), that.parameter());
//        }
//
//        @Override
//        public int hashCode() {
//            return Objects.hashCode(parameter());
//        }
//
//        private static <R extends Message> Function<FilterNode<R>, StructuredQuery.Filter>
//        toFilterFunction(FilterAdapter adapter) {
//            return new ToFilter<>(adapter);
//        }
//
//        private static class ToFilter<R extends Message>
//                implements Function<FilterNode<R>, StructuredQuery.Filter> {
//
//            private final FilterAdapter adapter;
//
//            private ToFilter(FilterAdapter adapter) {
//                this.adapter = adapter;
//            }
//
//            @Override
//            public StructuredQuery.Filter apply(@Nullable FilterNode<R> input) {
//                checkNotNull(input);
//                StructuredQuery.Filter result = input.toFilter(adapter);
//                return result;
//            }
//        }
//    }

//    private static <R extends Message> StructuredQuery.PropertyFilter
//    createFilter(FilterAdapter adapter,SubjectParameter<R, ?, ?> parameter) {
//        ColumnMapping<Value<?>> mapping = adapter.columnMapping();
//        return createFilter(parameter, mapping);
//    }

//    /**
//     * A specific type of a {@code FilterNode} tree node representing the tree head.
//     */
//    private static final class FilterTreeHead<R extends Message> extends FilterNode<R> {
//
//        private static <R extends Message> FilterNode<R> instance() {
//            return new FilterTreeHead<>();
//        }
//
//        /**
//         * {@inheritDoc}
//         *
//         * <p>A tree head is only equal to itself.
//         *
//         * @implNote Uses reference equality.
//         */
//        @Override
//        public boolean equals(Object o) {
//            return o == this;
//        }
//
//        @Override
//        public int hashCode() {
//            return 0;
//        }
//    }

//    /**
//     * A functional interface defining the operation of precessing a conjunction group found by
//     * the {@linkplain #fromPredicate parenthesis simplifying algorithm}.
//     *
//     * <p>Receives a {@code Collection} of {@linkplain FilterNode FilterNodes}
//     * representing a single generated conjunction group.
//     *
//     * @see FilterReducer for the implementation
//     */
//    @FunctionalInterface
//    private interface TreePathWalker<R extends Message> {
//
//        /**
//         * Walks a path in {@linkplain #conjunctionTree the conjunction tree} and performs
//         * a specified action for the given path.
//         *
//         * <p>The given path represents a single conjunction group.
//         *
//         * @param conjunctionGroup
//         *         the path in
//         *         the {@linkplain #conjunctionTree conjunction tree}
//         *         representing a single conjunction group
//         */
//        void walk(Collection<FilterNode<R>> conjunctionGroup);
//    }
}
