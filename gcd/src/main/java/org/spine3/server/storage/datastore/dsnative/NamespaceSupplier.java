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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import org.spine3.server.storage.datastore.DatastoreStorageFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A supplier for the {@linkplain Namespace namespaces}, based on the current multitenancy configuration and
 * {@linkplain org.spine3.users.TenantId tenant ID}.
 *
 * @author Dmytro Dashenkov
 */
abstract class NamespaceSupplier implements Supplier<Namespace> {

    /**
     * Obtains an instance of {@code NamespaceSupplier} for the passed
     * {@linkplain DatastoreStorageFactory storage factory}.
     *
     * @param factory the {@linkplain DatastoreStorageFactory storage factory} to return a supplier for
     * @see org.spine3.server.storage.StorageFactory#isMultitenant
     */
    static NamespaceSupplier instanceFor(DatastoreStorageFactory factory) {
        checkNotNull(factory);
        if (factory.isMultitenant()) {
            return Singleton.INSTANCE.singleTenant;
        } else {
            return Singleton.INSTANCE.multipleTenant;
        }
    }

    @VisibleForTesting
    static NamespaceSupplier constant() {
        return Singleton.INSTANCE.singleTenant;
    }

    @VisibleForTesting
    static NamespaceSupplier multitenant() {
        return Singleton.INSTANCE.multipleTenant;
    }

    /**
     * Generates a {@link Namespace} based on the current {@linkplain org.spine3.users.TenantId tenant ID}.
     *
     * @return an instance of {@link Namespace} representing either the current tenant ID or an empty string if
     * the {@linkplain DatastoreStorageFactory storage factory} passed upon the initialization is configured to be
     * single tenant
     */
    @Override
    public abstract Namespace get();

    private enum Singleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final NamespaceSupplier singleTenant = new SingleTenantNamespaceSupplier();
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final NamespaceSupplier multipleTenant = new MultitenantNamespaceSupplier();
    }
}
