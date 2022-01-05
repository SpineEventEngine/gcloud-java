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

import com.google.protobuf.Message;
import io.spine.query.RecordQuery;
import io.spine.server.storage.datastore.DatastoreMedium;
import io.spine.server.storage.datastore.record.DsEntitySpec;

/**
 * Factory for record lookup methods.
 *
 * @param <I>
 *         the type of the identifiers of the searched records
 * @param <R>
 *         the type of the searched records
 */
public final class DsLookup<I, R extends Message> {

    private final DatastoreMedium datastore;
    private final FilterAdapter adapter;
    private final DsEntitySpec<I, R> spec;

    private DsLookup(DatastoreMedium datastore, FilterAdapter adapter, DsEntitySpec<I, R> spec) {
        this.datastore = datastore;
        this.adapter = adapter;
        this.spec = spec;
    }

    /**
     * Creates a new instance of lookup method.
     *
     * @param datastore
     *         a facade to Datastore
     * @param adapter
     *         adapter for the column values to use in Datastore filters
     * @param spec
     *         the specification telling how the Protobuf Message is stored in terms of Datastore
     *         Entities
     * @param <I>
     *         the type of identifiers of the searched records
     * @param <R>
     *         the type of searched records
     * @return a new instance of Datastore lookup
     */
    public static <I, R extends Message> DsLookup<I, R>
    onTopOf(DatastoreMedium datastore, FilterAdapter adapter, DsEntitySpec<I, R> spec) {
        return new DsLookup<>(datastore, adapter, spec);
    }

    /**
     * Prepares the record query for execution with Datastore by transforming it into
     * an optimal form in terms of Datastore's native query language.
     *
     * @param query
     *         the query for which execution to prepare
     * @return a new instance of prepared query
     */
    public PreparedQuery<I, R> with(RecordQuery<I, R> query) {
        var subject = query.subject();
        if (!subject.id()
                    .values()
                    .isEmpty()) {
            return new DsLookupByIds<>(datastore, query, adapter, spec);
        }
        return new DsLookupByQueries<>(datastore, query, adapter, spec);
    }
}
