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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import io.spine.annotation.Internal;
import io.spine.core.TenantId;
import io.spine.server.storage.datastore.DatastoreStorageFactory;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A supplier for the {@linkplain Namespace namespaces}, based on the current multitenancy
 * configuration and {@linkplain TenantId tenant ID}.
 */
@Internal
public abstract class NamespaceSupplier implements Supplier<Namespace> {

    /** Avoid direct initialization from outside the package. */
    NamespaceSupplier() {
    }

    /**
     * Obtains an instance of {@code NamespaceSupplier} for the passed arguments.
     *
     * //TODO:2019-07-09:alex.tymchenko: address the ambiguous parameters.
     */
    public static NamespaceSupplier instance(boolean multitenant,
                                             @Nullable String defaultNamespace,
                                             NsConverterFactory converterFactory) {
        checkNotNull(converterFactory);
        if (multitenant) {
            return multitenant(converterFactory);
        } else {
            return new SingleTenantNamespaceSupplier(defaultNamespace);
        }
    }

    /**
     * Obtains an instance of {@code NamespaceSupplier} for the passed arguments.
     */
    public static NamespaceSupplier instance(boolean multitenant,
                                             @Nullable String defaultNamespace) {
        if (multitenant) {
            return multitenant(NsConverterFactory.defaults());
        } else {
            return new SingleTenantNamespaceSupplier(defaultNamespace);
        }
    }

    @VisibleForTesting
    static NamespaceSupplier singleTenant() {
        return new SingleTenantNamespaceSupplier(null);
    }

    @VisibleForTesting
    static NamespaceSupplier multitenant(NsConverterFactory converterFactory) {
        return MultitenantNamespaceSupplier.withConvertersBy(converterFactory);
    }

    /**
     * Generates a {@link Namespace} based on the current {@linkplain TenantId tenant ID}.
     *
     * @return an instance of {@link Namespace} representing either the current tenant ID or the
     *         default namespace if the {@linkplain DatastoreStorageFactory storage factory} passed
     *         upon the initialization is configured to be single tenant
     */
    @Override
    public abstract Namespace get();

    /**
     * Shown if this instance of {@code NamespaceSupplier} is multitenant or not.
     *
     * @return {@code true} if this supplier is multitenant, {@code false} otherwise
     */
    public abstract boolean isMultitenant();
}
