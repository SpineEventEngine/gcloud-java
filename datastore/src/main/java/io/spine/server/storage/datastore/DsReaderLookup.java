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

import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.ImmutableList;
import io.spine.logging.Logging;
import io.spine.server.storage.datastore.tenant.Namespace;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.min;

/**
 * A low-level Datastore lookup.
 *
 * <p>Uses a given {@link DatastoreReader} to find requested methods.
 */
final class DsReaderLookup implements Logging {

    private static final int MAX_KEYS_PER_READ_REQUEST = 1000;

    private final DatastoreReader datastore;

    DsReaderLookup(DatastoreReader datastore) {
        this.datastore = checkNotNull(datastore);
    }

    <R> DsQueryIterator<R> execute(StructuredQuery<R> query, Namespace namespace) {
        checkNotNull(query);
        checkNotNull(namespace);

        var queryWithNamespace = query.toBuilder()
                .setNamespace(namespace.value())
                .build();
        var iterator = new DsQueryIterator<>(queryWithNamespace, datastore);
        iterator._trace()
                .log("Reading the records of `%s` kind in `%s` namespace.",
                     query.getKind(), namespace.value());
        return iterator;
    }

    /**
     * Reads multiple records by their IDs.
     *
     * <p>The order of the resulting list is the same as the order of the keys. For keys which are
     * not in the database, {@code null} values are returned.
     */
    List<@Nullable Entity> find(Collection<Key> keys) {
        var keysList = ImmutableList.copyOf(keys);
        var entities = keysList.size() <= MAX_KEYS_PER_READ_REQUEST
                       ? fetch(keysList)
                       : readBulk(keysList);
        return entities;
    }

    /**
     * Reads big number of records.
     *
     * <p>Google App Engine Datastore has a limitation on the amount of entities queried with a
     * single call — 1000 entities per query. To deal with this limitation we read the entities in
     * pagination fashion 1000 entity per page.
     *
     * @param keys
     *         {@link Key keys} to find the entities for
     * @return ordered sequence of {@link Entity entities}
     */
    private List<Entity> readBulk(List<Key> keys) {
        var pageCount = keys.size() / MAX_KEYS_PER_READ_REQUEST + 1;
        _trace().log("Reading a big bulk of records synchronously. The data is read as %d pages.",
                     pageCount);
        var lowerBound = 0;
        var higherBound = MAX_KEYS_PER_READ_REQUEST;
        var keysLeft = keys.size();
        List<Entity> result = new ArrayList<>(keys.size());
        for (var i = 0; i < pageCount; i++) {
            var keysPage = keys.subList(lowerBound, higherBound);
            var page = fetch(keysPage);
            result.addAll(page);

            keysLeft -= keysPage.size();
            lowerBound = higherBound;
            higherBound += min(keysLeft, MAX_KEYS_PER_READ_REQUEST);
        }

        return result;
    }

    private List<Entity> fetch(List<Key> keys) {
        var keysArray = new Key[keys.size()];
        keys.toArray(keysArray);
        return datastore.fetch(keysArray);
    }
}
