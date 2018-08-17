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

package io.spine.server.datastore;

import com.google.cloud.datastore.Datastore;
import io.spine.server.BoundedContext;
import io.spine.server.BoundedContextBuilder;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.DatastoreStorageFactory;
import io.spine.server.storage.datastore.tenant.DatastoreTenants;
import io.spine.server.tenant.TenantIndex;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A factory for the instances of {@link BoundedContext} based on Datastore.
 *
 * @author Dmytro Dashenkov
 */
public final class Contexts {

    private Contexts() {
        // Prevent initialization of this utility class.
    }

    /**
     * Creates new instance of the {@link BoundedContextBuilder} based on the passed
     * {@link DatastoreStorageFactory}.
     *
     * <p>The returned instance has the following attributes pre-configured:
     * <ul>
     *     <li>{@link BoundedContextBuilder#setStorageFactorySupplier(java.util.function.Supplier)}
     *     StorageFactory};
     *     <li>{@link BoundedContextBuilder#setTenantIndex(TenantIndex) TenantIndex};
     *     <li>{@linkplain BoundedContextBuilder#setMultitenant(boolean) multitenancy}.
     * </ul>
     *
     * <p>In a majority of use cases the configuration of the produced
     * {@link BoundedContextBuilder builder} is enough for operation. However, it is still possible
     * to use the returned instance for further customization.
     *
     * @param storageFactory the {@link StorageFactory} to use in the result {@link BoundedContext}
     * @return new instance of {@link BoundedContextBuilder} with the specified parameters
     */
    public static BoundedContextBuilder onTopOf(DatastoreStorageFactory storageFactory) {
        checkNotNull(storageFactory);
        Datastore datastore = storageFactory.getDatastore()
                                                  .getDatastoreOptions()
                                                  .getService();
        TenantIndex tenantIndex = DatastoreTenants.index(datastore);
        Supplier<StorageFactory> storageFactorySupplier = () -> storageFactory;
        BoundedContextBuilder resultBuilder =
                BoundedContext.newBuilder()
                              .setMultitenant(storageFactory.isMultitenant())
                              .setStorageFactorySupplier(storageFactorySupplier)
                              .setTenantIndex(tenantIndex);
        return resultBuilder;
    }
}
