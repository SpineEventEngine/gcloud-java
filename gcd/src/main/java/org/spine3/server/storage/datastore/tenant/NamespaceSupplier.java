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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import org.spine3.annotations.Internal;
import org.spine3.server.storage.datastore.DatastoreStorageFactory;
import org.spine3.users.TenantId;

import javax.annotation.Nullable;

/**
 * A supplier for the {@linkplain Namespace namespaces}, based on the current multitenancy configuration and
 * {@linkplain TenantId tenant ID}.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public abstract class NamespaceSupplier implements Supplier<Namespace> {

    NamespaceSupplier() {
        // Avoid direct initialization from outside the package
    }

    /**
     * Obtains an instance of {@code NamespaceSupplier} for the passed
     * {@linkplain DatastoreStorageFactory storage factory}.
     *
     * @see org.spine3.server.storage.StorageFactory#isMultitenant
     */
    public static NamespaceSupplier instance(boolean multitenant, @Nullable String defaultNamespace) {
        if (multitenant) {
            return multitenant();
        } else {
            return new SingleTenantNamespaceSupplier(defaultNamespace);
        }
    }

    public static NamespaceSupplier singleTenant() {
        return new SingleTenantNamespaceSupplier(null);
    }

    @VisibleForTesting
    static NamespaceSupplier multitenant() {
        return MultitenantNamespaceSupplier.instance();
    }

    /**
     * Generates a {@link Namespace} based on the current {@linkplain TenantId tenant ID}.
     *
     * @return an instance of {@link Namespace} representing either the current tenant ID or the
     * default namespace if the {@linkplain DatastoreStorageFactory storage factory} passed upon
     * the initialization is configured to be single tenant
     */
    @SuppressWarnings("AbstractMethodOverridesAbstractMethod")
        // Overrides to provide a descriptive documentation
    @Override
    public abstract Namespace get();
}
