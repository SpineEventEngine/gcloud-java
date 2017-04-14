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

package org.spine3.server.storage.datastore.tenant;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.spine3.server.storage.datastore.Kind;
import org.spine3.server.tenant.TenantIndex;
import org.spine3.users.TenantId;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A DAO for the Datastore {@link Namespace Namespaces}.
 *
 * @author Dmytro Dashenkov
 */
@ThreadSafe
class NamespaceIndex implements TenantIndex {

    private static final Kind NAMESPACE_KIND = Kind.ofNamespace();

    private final Datastore datastore;

    private final Set<Namespace> cache = new HashSet<>();

    private final Object lock = new Object();

    NamespaceIndex(Datastore datastore) {
        this.datastore = datastore;
    }

    /**
     * {@inheritDoc}
     *
     * <p>If the ID is not found, writes the ID into the in-mem cache.
     *
     * @param id the ID to ensure
     */
    @Override
    public void keep(TenantId id) {
        checkNotNull(id);
        synchronized (lock) {
            cache.add(Namespace.of(id));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<TenantId> getAll() {
        synchronized (lock) {
            fetchNamespaces();
        }
        final Set<TenantId> result = new HashSet<>(cache.size());
        for (Namespace namespace : cache) {
            if (namespace != null) {
                result.add(namespace.toTenantId());
            }
        }
        return result;
    }

    /**
     * Preforms no action.
     */
    @Override
    public void close() {
        // NOP
    }

    /**
     * Checks if the Datastore has the given {@linkplain Namespace}, i.e. there is at least one
     * {@linkplain com.google.cloud.datastore.Entity Entity} in this {@linkplain Namespace}.
     *
     * @param namespace the {@linkplain Namespace} yo look for
     * @return {@code true} if there is at least one
     * {@linkplain com.google.cloud.datastore.Entity Entity} in this {@linkplain Namespace} or the corresponding
     * {@link TenantId} has been put into the index by a call to {@link #keep(TenantId)},
     * {@code false} otherwise
     */
    boolean contains(Namespace namespace) {
        checkNotNull(namespace);

        if (namespace.getValue()
                     .isEmpty()) { // Default namespace, always exists
            return true;
        }

        synchronized (lock) {
            final boolean cachedNamespace = cache.contains(namespace);
            if (cachedNamespace) {
                return true;
            }

            fetchNamespaces();
            final boolean result = cache.contains(namespace);
            return result;
        }
    }

    /**
     * Fetches the namespaces from the Datastore into the in-mem cache.
     */
    private void fetchNamespaces() {
        final Query<Key> query = Query.newKeyQueryBuilder()
                                      .setKind(NAMESPACE_KIND.getValue())
                                      .build();
        final Iterator<Key> existingNamespaces = datastore.run(query);
        final Set<Namespace> newNamespaces = new HashSet<>();
        final Iterator<Namespace> extractedNamespaces =
                Iterators.transform(existingNamespaces, new NamespaceUnpacker());
        Iterators.addAll(newNamespaces, extractedNamespaces);

        // Never delete tenants, only add new ones
        cache.addAll(newNamespaces);
    }

    /**
     * A function retrieving a {@link Namespace} from a given {@link Key}.
     */
    private static class NamespaceUnpacker implements Function<Key, Namespace> {

        /**
         * Retrieves a {@link Namespace} from a given {@link Key}.
         *
         * @param key a Datastore {@link Key} representing a Datastore namespace
         * @return the result of call to {@link Key#getName()} or {@code null} if the
         * {@link Key} has no name (i.e. for the default namespace)
         */
        @Nullable
        @Override
        public Namespace apply(@Nullable Key key) {
            checkNotNull(key);
            return Namespace.fromNameOf(key);
        }
    }
}