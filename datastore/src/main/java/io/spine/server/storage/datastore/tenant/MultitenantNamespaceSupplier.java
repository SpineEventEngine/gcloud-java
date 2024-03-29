/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import io.spine.core.TenantId;
import io.spine.server.tenant.TenantFunction;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

/**
 * A {@link NamespaceSupplier} for multitenant storage factories.
 */
final class MultitenantNamespaceSupplier extends NamespaceSupplier {

    private final NsConverterFactory converterFactory;

    static NamespaceSupplier withConvertersBy(NsConverterFactory converterFactory) {
        return new MultitenantNamespaceSupplier(converterFactory);
    }

    private MultitenantNamespaceSupplier(NsConverterFactory converterFactory) {
        super();
        this.converterFactory = checkNotNull(converterFactory);
    }

    /**
     * Obtains a namespace for the current tenant {@link TenantId ID}.
     *
     * @return the {@code Namespace} value
     */
    @Override
    public Namespace get() {
        var retriever = new TenantIdRetriever();
        var tenantId = retriever.execute();
        requireNonNull(tenantId);
        var result = Namespace.of(tenantId, true, converterFactory);
        return result;
    }

    @Override
    public boolean isMultitenant() {
        return true;
    }

    /**
     * A function disclosing the current tenant {@linkplain TenantId ID}.
     */
    private static class TenantIdRetriever extends TenantFunction<TenantId> {

        /**
         * Creates a new instance of {@code TenantIdRetriever}.
         *
         * @throws IllegalStateException
         *         if the application has a single tenant
         */
        private TenantIdRetriever() throws IllegalStateException {
            super(true);
        }

        /**
         * Retrieves the passed {@link TenantId}, ensuring it's not equal to {@code null}.
         *
         * @param input
         *         current {@link TenantId}
         * @return the input
         */
        @Override
        public TenantId apply(@Nullable TenantId input) {
            checkNotNull(input);
            return input;
        }
    }
}
