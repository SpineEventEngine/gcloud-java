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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.Streams;
import io.spine.string.Stringifiers;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility for generating the
 * {@linkplain io.spine.server.storage.Storage#index() storage ID indexes}.
 *
 * @see io.spine.server.storage.Storage#index()
 */
final class Indexes {

    /**
     * Prevents the utility class instantiation.
     */
    private Indexes() {
    }

    /**
     * Retrieves the ID index for the given {@code kind} or the storage records.
     *
     * @param datastore
     *         datastore to get the indexes for
     * @param kind
     *         the kind if the records in the datastore
     * @param <I>
     *         type of the IDs to retrieve
     * @return an {@link Iterator} of the IDs matching given record kind
     */
    static <I> Iterator<I> indexIterator(DatastoreWrapper datastore, Kind kind, Class<I> idType) {
        checkNotNull(datastore);
        checkNotNull(kind);
        checkNotNull(idType);

        StructuredQuery<Key> query = Query.newKeyQueryBuilder()
                                          .setKind(kind.value())
                                          .build();
        Iterator<Key> allEntities = datastore.read(query);
        Iterator<I> idIterator = Streams.stream(allEntities)
                                        .map(idExtractor(idType))
                                        .iterator();
        return idIterator;
    }

    private static <I> Function<Key, @Nullable I> idExtractor(Class<I> idType) {
        return key -> {
            checkNotNull(key);
            String stringId = key.getName();
            I id = Stringifiers.fromString(stringId, idType);
            return id;
        };
    }
}
