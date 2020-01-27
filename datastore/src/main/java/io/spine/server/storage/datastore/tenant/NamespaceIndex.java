/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore.tenant;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import io.spine.core.TenantId;
import io.spine.server.BoundedContext;
import io.spine.server.storage.datastore.Kind;
import io.spine.server.tenant.TenantIndex;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;
import static io.spine.server.storage.datastore.tenant.NamespaceConverter.NOT_A_TENANT;

/**
 * A DAO for the Datastore {@link Namespace Namespaces}.
 */
@ThreadSafe
final class NamespaceIndex implements TenantIndex {

    private static final Kind NAMESPACE_KIND = Kind.ofNamespace();

    private final Set<Namespace> cache = new HashSet<>();
    private final Object lock = new Object();
    private final NamespaceQuery namespaceQuery;
    private final NsConverterFactory converterFactory;
    private final boolean multitenant;

    private boolean registered;

    NamespaceIndex(Datastore datastore, boolean multitenant, NsConverterFactory converterFactory) {
        this(new DefaultNamespaceQuery(datastore),
             multitenant,
             converterFactory
        );
    }

    NamespaceIndex(NamespaceQuery namespaceQuery,
                   boolean multitenant,
                   NsConverterFactory converterFactory) {
        this.namespaceQuery = checkNotNull(namespaceQuery);
        this.converterFactory = converterFactory;
        this.multitenant = multitenant;
    }

    @Override
    public void registerWith(BoundedContext context) {
        checkNotNull(context);
        registered = true;
        // Do nothing more, as this implementation does not rely on any `BoundedContext` properties.
    }

    @Override
    public boolean isRegistered() {
        return registered;
    }

    /**
     * {@inheritDoc}
     *
     * <p>If the ID is not found, writes the ID into the in-mem cache.
     *
     * @param id
     *         the ID to ensure
     */
    @Override
    public void keep(TenantId id) {
        checkNotNull(id);
        synchronized (lock) {
            cache.add(Namespace.of(id, multitenant, converterFactory));
        }
    }

    @Override
    public Set<TenantId> all() {
        synchronized (lock) {
            fetchNamespaces();

            Set<TenantId> result = new HashSet<>(cache.size());
            for (Namespace namespace : cache) {
                if (namespace != null) {
                    TenantId tenantId = namespace.toTenantId();
                    if (!NOT_A_TENANT.equals(tenantId)) {
                        result.add(tenantId);
                    }
                }
            }
            return result;
        }
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
     * {@linkplain Entity Entity} in this {@linkplain Namespace}.
     *
     * @param namespace
     *         the {@linkplain Namespace} to look for
     * @return {@code true} if there is at least one
     *         {@linkplain Entity Entity} in this {@linkplain Namespace} or the corresponding
     *         {@link TenantId} has been put into the index by a call to {@link #keep(TenantId)},
     *         {@code false} otherwise
     */
    boolean contains(Namespace namespace) {
        checkNotNull(namespace);

        if (namespace.value().isEmpty()) { // Default namespace, always exists
            return true;
        }

        synchronized (lock) {
            boolean cachedNamespace = cache.contains(namespace);
            if (cachedNamespace) {
                return true;
            }

            fetchNamespaces();
            boolean result = cache.contains(namespace);
            return result;
        }
    }

    /**
     * Fetches the namespaces from the Datastore into the in-mem cache.
     */
    private void fetchNamespaces() {
        Iterator<Key> existingNamespaces = namespaceQuery.run();
        Set<Namespace> newNamespaces = newHashSet();
        NamespaceUnpacker unpacker = new NamespaceUnpacker(multitenant, converterFactory);
        Iterator<Namespace> extractedNamespaces = Iterators.transform(existingNamespaces, unpacker);
        Iterators.addAll(newNamespaces, extractedNamespaces);

        // Never delete tenants, only add new ones
        cache.addAll(newNamespaces);
    }

    /**
     * A Datastore query retrieving all the existing namespaces in form of Datastore
     * {@link Key keys}.
     *
     * <p>The {@code name} field of the {@link Key keys} contains the Datastore namespace name.
     */
    public interface NamespaceQuery {

        /**
         * Runs the Datastore query.
         *
         * @return an {@link Iterator} of the Datastore {@link Key keys} representing
         *         the namespaces.
         */
        Iterator<Key> run();
    }

    /**
     * A default implementation of the {@link NamespaceQuery}.
     *
     * <p>Delegates to the Datastore {@link KeyQuery KeyQuery}.
     */
    private static class DefaultNamespaceQuery implements NamespaceQuery {

        private final Datastore datastore;

        private DefaultNamespaceQuery(Datastore datastore) {
            this.datastore = checkNotNull(datastore);
        }

        @Override
        public Iterator<Key> run() {
            Query<Key> query = Query.newKeyQueryBuilder()
                                    .setKind(NAMESPACE_KIND.value())
                                    .build();
            Iterator<Key> result = datastore.run(query);
            return result;
        }
    }

    /**
     * A function retrieving a {@link Namespace} from a given {@link Key}.
     */
    private static class NamespaceUnpacker implements Function<Key, Namespace> {

        private final boolean multitenant;
        private final NsConverterFactory converterFactory;

        private NamespaceUnpacker(boolean multitenant,
                                  NsConverterFactory factory) {
            this.multitenant = multitenant;
            converterFactory = factory;
        }

        /**
         * Retrieves a {@link Namespace} from a given {@link Key}.
         *
         * @param key
         *         a Datastore {@link Key} representing a Datastore namespace
         * @return the result of call to {@link Key#getName()} or {@code null} if the
         *         {@link Key} has no name (i.e. the namespace is default)
         */
        @Override
        public @Nullable Namespace apply(@Nullable Key key) {
            checkNotNull(key);
            return Namespace.fromNameOf(key, multitenant, converterFactory);
        }
    }
}
