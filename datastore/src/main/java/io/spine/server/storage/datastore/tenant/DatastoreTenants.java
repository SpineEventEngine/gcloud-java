/*
 * Copyright 2019, TeamDev. All rights reserved.
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
import io.spine.server.tenant.TenantIndex;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A factory of the Datastore-specific Tenant related objects.
 *
 * @author Dmytro Dashenkov
 */
public final class DatastoreTenants {

    /**
     * Prevents the utility class instantiation.
     */
    private DatastoreTenants() {
    }

    /**
     * Creates a {@link TenantIndex} for the given {@link Datastore}.
     *
     * <p>In a multitenant application it is necessary to pass an instance of
     * the {@link TenantIndex} to a {@link io.spine.server.BoundedContextBuilder
     * BoundedContextBuilder} when creating an instance of {@link io.spine.server.BoundedContext
     * BoundedContext}.
     *
     * <p>An example of creating a multitenant {@link io.spine.server.BoundedContext BoundedContext}
     * with the tenant index built on top of Datastore:
     * <pre>
     * {@code
     *     Datastore datastore = myDatastoreOptions.getService();
     *
     *     // Configure the `ServerEnvironment` with the factory on top of the Datastore instance.
     *     // ...
     *
     *     // Build the `TenantIndex` on top of the same Datastore instance.
     *     TenantIndex myTenantIndex = DatastoreTenants.index(datastore);
     *
     *
     *     BoundedContext context = BoundedContext.multitenant("Code samples")
     *                                            .setTenantIndex(myTenantIndex)
     *                                             .build();
     * }
     * </pre>
     *
     * <p>A single-tenant app (or a single-tenant BoundedContext in a multitenant app) does not
     * require a {@code TenantIndex} to be set explicitly, so this method assumes that it is an a
     * single tenant context.
     *
     * @param datastore
     *         the {@link Datastore} to get the {@link TenantIndex} for
     * @return a new instance of the {@link TenantIndex}
     * @see io.spine.server.storage.datastore.DatastoreStorageFactory#configureTenantIndex(io.spine.server.BoundedContextBuilder)
     *         for an alternative method, suitable if you already have an instance of
     *         {@code BoundedContextBuilder}
     */
    public static TenantIndex index(Datastore datastore) {
        checkNotNull(datastore);
        // We assume we are in a multi-tenant execution environment
        TenantIndex index = new NamespaceIndex(datastore, true);
        return index;
    }
}
