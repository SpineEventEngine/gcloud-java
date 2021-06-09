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

import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * A {@link RecordQuery} prepared for optimal execution in terms of Datastore
 * {@link com.google.cloud.datastore.Query Query} language.
 *
 * <p>Due to Datastore limitations, some {@code RecordQuery} instances are processed partly
 * via the Datastore querying, and partly by filtering the intermediate results in memory.
 */
@Internal
public abstract class PreparedQuery<I, R extends Message> {

    private final RecordQuery<I, R> query;
    private final TypeUrl recordType;
    private final FilterAdapter columnAdapter;
    private final DsEntitySpec<I, R> spec;

    PreparedQuery(RecordQuery<I, R> query, FilterAdapter adapter, DsEntitySpec<I, R> spec) {
        this.query = query;
        this.recordType = TypeUrl.of(query.subject()
                                          .recordType());
        columnAdapter = adapter;
        this.spec = spec;
    }

    //TODO:2021-04-13:alex.tymchenko: document.
    public final Iterable<R> execute() {
        IntermediateResult intermediateResult = fetchFromDatastore();
        Iterable<R> result = toRecords(intermediateResult);
        return result;
    }

    public final Iterable<R> readRecords() {
        IntermediateResult intermediateResult = fetchFromDatastore();
        Iterable<R> result = toRecords(intermediateResult);
        return result;
    }

    /**
     * Queries Datastore for the {@code RecordQuery} part which may be processed by Datastore means.
     *
     * <p>Returns an intermediate result to be processed further.
     */
    protected abstract IntermediateResult fetchFromDatastore();

    /**
     * Turns the intermediate results obtained from Datastore into the desired format of records.
     *
     * <p>Some complex {@code RecordQuery} instances may require additional in-memory processing
     * at this stage. Other queries typically only require the conversion of data format.
     */
    protected abstract Iterable<R> toRecords(IntermediateResult result);

    protected final RecordQuery<I, R> query() {
        return query;
    }

    protected final TypeUrl recordType() {
        return recordType;
    }

    protected final ImmutableList<SortBy<?, R>> sorting() {
        return query.sorting();
    }

    protected final boolean hasOrdering() {
        return !sorting().isEmpty();
    }

    protected final QueryPredicate<R> predicate() {
        return query.subject().predicate();
    }

    protected final ImmutableSet<I> identifiers() {
        return query.subject()
                    .id()
                    .values();
    }

    protected final @Nullable Integer limit() {
        return query.limit();
    }

    protected final FieldMask mask() {
        return query.mask();
    }

    protected final FilterAdapter columnAdapter() {
        return columnAdapter;
    }

    protected final DsEntitySpec<I, R> spec() {
        return spec;
    }

    /**
     * The result obtained from Datastore directly by sending one or more queries to it.
     *
     * <p>In order to be returned as a lookup result, needs to be post-processed in memory.
     */
    static final class IntermediateResult {

        private final List<@Nullable Entity> entities;

        /**
         * Creates a new instance by referencing (not copying) the given list of Entities.
         *
         * @param entities
         *         list of Datastore entities, some of which may be {@code null} in case
         *         they were queried by identifiers, and the requested records
         *         were missing from the underlying storage
         * @apiNote This ctor does not utilize an {@code ImmutableList},
         *         as it cannot contain {@code null}s.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")  /* To improve performance. */
        IntermediateResult(List<@Nullable Entity> entities) {
            this.entities = entities;
        }

        List<@Nullable Entity> entities() {
            return unmodifiableList(entities);
        }
    }
}
