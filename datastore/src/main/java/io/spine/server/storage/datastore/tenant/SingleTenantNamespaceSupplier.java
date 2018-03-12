/*
 * Copyright 2018, TeamDev Ltd. All rights reserved.
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

import javax.annotation.Nullable;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * A {@link NamespaceSupplier} for single-tenant storage factories.
 *
 * @author Dmytro Dashenkov
 */
final class SingleTenantNamespaceSupplier extends NamespaceSupplier {

    private static final String DEFAULT_NAMESPACE = "";

    private final Namespace namespace;

    SingleTenantNamespaceSupplier(@Nullable String namespace) {
        super();
        this.namespace = isNullOrEmpty(namespace)
                         ? NamespaceSingleton.INSTANCE.value
                         : Namespace.of(namespace);
    }

    /**
     * {@inheritDoc}
     *
     * @return the {@link Namespace} representing the empty string
     */
    @Override
    public Namespace get() {
        return namespace;
    }

    @Override
    public boolean isMultitenant() {
        return false;
    }

    private enum NamespaceSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Namespace value = Namespace.of(DEFAULT_NAMESPACE);
    }
}
