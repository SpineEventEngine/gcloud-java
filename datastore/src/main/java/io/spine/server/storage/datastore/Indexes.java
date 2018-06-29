/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import io.spine.server.storage.Storage;
import io.spine.string.Stringifiers;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility for generating the
 * {@linkplain Storage#index() storage ID indexes}.
 *
 * @author Dmytro Dashenkov.
 * @see Storage#index()
 */
public final class Indexes {

    /**
     * Prevents the utility class instantiation.
     */
    private Indexes() {
    }

    /**
     * Retrieves the ID index for the given {@code kind} or the storage records.
     *
     * @param datastore datastore to get the indexes for
     * @param kind      the kind if the records in the datastore
     * @param <I>       type of the IDs to retrieve
     * @return an {@link Iterator} of the IDs matching given record kind
     */
    public static <I> Iterator<I> indexIterator(DatastoreWrapper datastore,
                                                Kind kind,
                                                Class<I> idType) {
        checkNotNull(datastore);
        checkNotNull(kind);
        checkNotNull(idType);

        EntityQuery.Builder query = Query.newEntityQueryBuilder()
                                               .setKind(kind.getValue());
        Iterator<Entity> allEntities = datastore.read(query.build());
        Iterator<I> idIterator = Streams.stream(allEntities)
                                        .map(idExtractor(idType))
                                        .iterator();
        return idIterator;
    }

    private static <I> Function<Entity, @Nullable I> idExtractor(Class<I> idType) {
        return input -> {
            checkNotNull(input);
            Key key = input.getKey();
            String stringId = key.getName();
            I id = Stringifiers.fromString(stringId, idType);
            return id;
        };
    }
}
