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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.query.QueryPredicate;
import io.spine.query.RecordQuery;
import io.spine.query.SortBy;
import io.spine.server.storage.datastore.record.DsEntitySpec;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link RecordQuery} prepared for optimal execution in terms of Datastore
 * {@link com.google.cloud.datastore.Query Query} language.
 *
 * <p>Due to Datastore limitations, some {@code RecordQuery} instances are processed partly
 * via the Datastore querying, and partly by filtering the intermediate results in memory.
 * This type analyzes the queries and determines the execution strategy.
 *
 * @param <I>
 *         the type of identifiers of the queried records
 * @param <R>
 *         the type of queried records
 */
@Internal
public abstract class PreparedQuery<I, R extends Message> {

    private final RecordQuery<I, R> query;
    private final TypeUrl recordType;
    private final FilterAdapter columnAdapter;
    private final DsEntitySpec<I, R> spec;

    /**
     * Creates a new instance.
     *
     * @param query
     *         an original {@code RecordQuery} to execute
     * @param adapter
     *         an adapter for the values set by Spine-specific predicates
     *         to those applicable to Datastore-native Filters
     * @param spec
     *         a specification of an Entity
     */
    PreparedQuery(RecordQuery<I, R> query, FilterAdapter adapter, DsEntitySpec<I, R> spec) {
        this.query = query;
        this.recordType = TypeUrl.of(query.subject()
                                          .recordType());
        columnAdapter = adapter;
        this.spec = spec;
    }

    /**
     * Executes the query and returns the read result.
     */
    public final Iterable<R> execute() {
        var intermediateResult = fetchFromDatastore();
        var result = toRecords(intermediateResult);
        return result;
    }

    /**
     * Queries Datastore for the {@code RecordQuery} part which may be processed by Datastore means.
     *
     * <p>Returns an intermediate result to be processed further.
     */
    abstract IntermediateResult fetchFromDatastore();

    /**
     * Turns the intermediate results obtained from Datastore into the desired format of records.
     *
     * <p>Some complex {@code RecordQuery} instances may require additional in-memory processing
     * at this stage. Other queries typically only require the conversion of data format.
     */
    abstract Iterable<R> toRecords(IntermediateResult result);

    /**
     * Returns the original {@code RecordQuery}.
     */
    final RecordQuery<I, R> query() {
        return query;
    }

    /**
     * Returns the type URL of the queried records.
     */
    final TypeUrl recordType() {
        return recordType;
    }

    /**
     * Returns the sorting directives of the original {@code RecordQuery}.
     */
    final ImmutableList<SortBy<?, R>> sorting() {
        return query.sorting();
    }

    /**
     * Tells whether the original {@code RecordQuery} has sorting directives.
     */
    final boolean hasSorting() {
        return !sorting().isEmpty();
    }

    /**
     * Returns the predicate of the original {@code RecordQuery}.
     */
    final QueryPredicate<R> predicate() {
        return query.subject()
                    .predicate();
    }

    /**
     * Returns the identifiers which were specified by the original {@code RecordQuery}.
     */
    final ImmutableSet<I> identifiers() {
        return query.subject()
                    .id()
                    .values();
    }

    /**
     * Returns the limit set by the original {@code RecordQuery}.
     *
     * <p>Returns {@code null} if no limit was set.
     */
    final @Nullable Integer limit() {
        return query.limit();
    }

    /**
     * Returns the field mask set by the original {@code RecordQuery}.
     */
    final FieldMask mask() {
        return query.mask();
    }

    /**
     * Returns the adapter from the {@code RecordQuery} predicates
     * to native Datastore {@code Filter}s.
     */
    final FilterAdapter columnAdapter() {
        return columnAdapter;
    }

    /**
     * Returns the specification of the Datastore Entity to use in querying.
     */
    final DsEntitySpec<I, R> spec() {
        return spec;
    }
}
