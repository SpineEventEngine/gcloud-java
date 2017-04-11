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

package org.spine3.server.storage.datastore.dsnative;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Query;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dmytro Dashenkov
 */
class NamespaceAccess {

    private static final Kind NAMESPACE_KIND = Kind.ofNamespace();
    private static final Function<Entity, Namespace> NAMESPACE_UNPACKER =
            new Function<Entity, Namespace>() {
                @Nullable
                @Override
                public Namespace apply(@Nullable Entity entity) {
                    checkNotNull(entity);

                    return null;
                }
            };

    private final Datastore datastore;

    private final Set<Namespace> cache = new HashSet<>();

    NamespaceAccess(Datastore datastore) {
        this.datastore = datastore;
    }

    boolean exists(Namespace namespace) {
        checkNotNull(namespace);

        if (namespace.getValue().isEmpty()) {
            return true;
        }
        final boolean cachedNamespace = cache.contains(namespace);
        if (cachedNamespace) {
            return true;
        }
        cache.clear();

        final EntityQuery query = Query.newEntityQueryBuilder()
                                       .setKind(NAMESPACE_KIND.getValue())
                                       .build();

        final Iterator<Entity> existingNamespaces = datastore.run(query);
        final Iterator<Namespace> extractedNamespaces =
                Iterators.transform(existingNamespaces, NAMESPACE_UNPACKER);
        Iterators.addAll(cache, extractedNamespaces);
        final boolean result = cache.contains(namespace);
        return result;
    }
}
