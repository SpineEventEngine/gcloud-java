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
import org.spine3.server.tenant.TenantIndex;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A factory of the Datastore-specific Tenant related objects.
 *
 * @author Dmytro Dashenkov
 */
public class DatastoreTenants {

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
     * <code>
     *     <pre>
     *         final Datastore myDatastoreConfig = myDatastoreOptions.getService();
     *
     *         // Create DatastoreStorageFactories using this instance of Datastore
     *         final Supplier{@literal <}StorageFactory{@literal >} dsStorageFactorySupplier = getDatastoreStorageFactorySupplier(myDatastoreConfig);
     *
     *         // Use the same instance for the TenantIndex
     *         final TenantIndex myTenantIndex = DatastoreTenants.index(myDatastoreConfig);
     *
     *         // Pass both to the BoundedContext.Builder
     *         final BoundedContext multitenantAppBc = BoundedContext.newBuilder()
     *                                                               .setStorageFactorySupplier(dsStorageFactorySupplier)
     *                                                               .setTenantIndex(myTenantIndex)
     *                                                               // set other prams
     *                                                               .build();
     *     </pre>
     * </code>
     *
     * <p>A single-tenant app (or a single-tenant BoundedContext in a multitenant app) does not
     * require a {@code TenantIndex} to be set explicitly.
     *
     * @param datastore the {@link Datastore} to get the {@link TenantIndex} for
     * @return a new instance of the {@link TenantIndex}
     */
    public static TenantIndex index(Datastore datastore) {
        checkNotNull(datastore);
        final TenantIndex index = new NamespaceAccess(datastore);
        return index;
    }
}