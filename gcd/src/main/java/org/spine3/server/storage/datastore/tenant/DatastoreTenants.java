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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.spine3.annotations.Internal;
import org.spine3.server.storage.datastore.ProjectId;
import org.spine3.server.tenant.TenantIndex;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.synchronizedMap;

/**
 * A factory of the Datastore-specific Tenant related objects.
 *
 * @author Dmytro Dashenkov
 */
public class DatastoreTenants {

    private static final Map<ProjectId, NamespaceToTenantIdConverter> tenantIdConverters =
            synchronizedMap(Maps.<ProjectId, NamespaceToTenantIdConverter>newHashMap());

    private DatastoreTenants() {
        // Prevent the utility class initialization
    }

    /**
     * Creates a {@link TenantIndex} for the given {@link Datastore}.
     *
     * <p>In a multitenant application it's necessary to pass an instance of
     * the {@link TenantIndex} to a
     * {@link org.spine3.server.BoundedContext.Builder BoundedContext.Builder}
     * when creating an instance of {@link org.spine3.server.BoundedContext BoundedContext}.
     *
     * <p>An example of creating a multitenant
     * {@link org.spine3.server.BoundedContext BoundedContext} using the Datastore Storage:
     * <pre>
     *     {@code
     *         final Datastore myDatastoreConfig = myDatastoreOptions.getService();
     *
     *         // Create DatastoreStorageFactories using this instance of Datastore
     *         final Supplier<StorageFactory> dsStorageFactorySupplier =
     *                                      getDatastoreStorageFactorySupplier(myDatastoreConfig);
     *
     *         // Use the same instance for the TenantIndex
     *         final TenantIndex myTenantIndex = DatastoreTenants.index(myDatastoreConfig);
     *
     *         // Pass both to the BoundedContext.Builder
     *         final BoundedContext multitenantAppBc =
     *                              BoundedContext.newBuilder()
     *                                            .setStorageFactorySupplier(dsStorageFactorySupplier)
     *                                            .setTenantIndex(myTenantIndex)
     *                                            .setMultitenant(true)
     *                                            // set other params
     *                                            .build();
     *     }
     * </pre>
     *
     * <p>A single-tenant app (or a single-tenant BoundedContext in a multitenant app) does not
     * require a {@code TenantIndex} to be set explicitly.
     *
     * @param datastore the {@link Datastore} to get the {@link TenantIndex} for
     * @return a new instance of the {@link TenantIndex}
     */
    public static TenantIndex index(Datastore datastore) {
        checkNotNull(datastore);
        final TenantIndex index = new NamespaceIndex(datastore);
        return index;
    }

    /**
     * Registers a {@link com.google.common.base.Converter Converter} from string datastore
     * namespace into {@link org.spine3.users.TenantId TenantId} for the given {@link ProjectId}.
     *
     * <p>After this converter has been registered, all the Datastore namespace operations will use
     * it instead of the {@linkplain Namespace default behavior}.
     *
     * <p>Note, that this method should be called only once per one instance of {@link ProjectId}.
     * All the subsequent invocations will cause {@code IllegalStateException}s.
     *
     * @param converter the converter to use for the
     *                  namespace-to-{@link org.spine3.users.TenantId TenantId} and vice versa
     *                  conversions
     * @see Namespace
     */
    @Internal
    public static void registerNamespaceConverter(ProjectId projectId,
                                                  NamespaceToTenantIdConverter converter) {
        checkNotNull(projectId);
        checkNotNull(converter);
        final NamespaceToTenantIdConverter pastConverter = tenantIdConverters.put(projectId,
                                                                                  converter);
        checkState(pastConverter == null,
                   "A namespace converter has already been registered.");
    }

    /**
     * Retrieves the registered {@link NamespaceToTenantIdConverter}.
     *
     * @return the {@linkplain #registerNamespaceConverter registered}
     * {@link NamespaceToTenantIdConverter} wrapped into {@link Optional} or
     * {@link Optional#absent() Optional.absent()} if the converter has never been registered
     */
    @VisibleForTesting
    public static Optional<NamespaceToTenantIdConverter> getNamespaceConverter(ProjectId projectId) {
        checkNotNull(projectId);
        final NamespaceToTenantIdConverter converter = tenantIdConverters.get(projectId);
        return Optional.fromNullable(converter);
    }
}
