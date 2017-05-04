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

package org.spine3.server.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.spine3.server.BoundedContext;
import org.spine3.server.storage.StorageFactory;
import org.spine3.server.storage.datastore.DatastoreStorageFactory;
import org.spine3.server.storage.datastore.tenant.DatastoreTenants;
import org.spine3.server.tenant.TenantIndex;

/**
 * A factory fot the instances of {@link BoundedContext} based on Datastore.
 *
 * @author Dmytro Dashenkov
 */
public final class Contexts {

    private Contexts() {
        // Prevent initialization of this utility class
    }

    /**
     * Creates new instance of the {@link BoundedContext.Builder} based of the passed
     * {@link DatastoreStorageFactory}.
     *
     * <p>The result builder will have the next parameters set:
     * <ul>
     *     <li>{@link BoundedContext.Builder#setStorageFactorySupplier(Supplier) StorageFactory};
     *     <li>{@link BoundedContext.Builder#setTenantIndex(TenantIndex) TenantIndex};
     *     <li>{@linkplain BoundedContext.Builder#setMultitenant(boolean) multitenancy}.
     * </ul>
     *
     * <p>For the most applications this config will be convenient if the passed
     * {@link DatastoreStorageFactory} is configured properly. Though, the successive calls to
     * the corresponding {@link BoundedContext.Builder Builder} methods may override this
     * configuration.
     *
     * @param storageFactory the {@link StorageFactory} to use in the result {@link BoundedContext}
     * @return new instance of the {@link BoundedContext.Builder} with the specified parameters
     */
    public static BoundedContext.Builder from(DatastoreStorageFactory storageFactory) {
        final Datastore datastore = storageFactory.getDatastore()
                                                  .getDatastoreOptions()
                                                  .getService();
        final TenantIndex tenantIndex = DatastoreTenants.index(datastore);
        final Supplier<StorageFactory> storageFactorySupplier =
                Suppliers.<StorageFactory>ofInstance(storageFactory);

        final BoundedContext.Builder resultBuilder =
                BoundedContext.newBuilder()
                              .setMultitenant(storageFactory.isMultitenant())
                              .setStorageFactorySupplier(storageFactorySupplier)
                              .setTenantIndex(tenantIndex);
        return resultBuilder;
    }
}
