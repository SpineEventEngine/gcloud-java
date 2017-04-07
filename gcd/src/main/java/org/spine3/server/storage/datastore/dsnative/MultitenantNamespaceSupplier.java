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

import org.spine3.server.tenant.TenantFunction;
import org.spine3.users.TenantId;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * A {@link NamespaceSupplier} for multitenant storage factories.
 *
 * @author Dmytro Dashenkov
 */
final class MultitenantNamespaceSupplier extends NamespaceSupplier {

    /**
     * {@inheritDoc}
     *
     * @return the {@link Namespace} representing the current tenant {@link TenantId ID}
     */
    @Override
    public Namespace getNamespace() {
        final TenantIdRetriever retriever = TenantIdRetriever.instance();
        final TenantId tenantId = retriever.execute();
        checkNotNull(tenantId);
        final String stringTenantId = tenantIdToSignificantString(tenantId);
        final Namespace result = Namespace.of(stringTenantId);
        return result;
    }

    /**
     * @return a human-friendly string representation of the {@link TenantId}
     */
    private static String tenantIdToSignificantString(TenantId id) {
        final TenantId.KindCase kindCase = id.getKindCase();
        switch (kindCase) {
            case DOMAIN:
                return id.getDomain()
                         .getValue();
            case EMAIL:
                return id.getEmail()
                         .getValue();
            case VALUE:
                return id.getValue();
            case KIND_NOT_SET:
            default:
                throw new IllegalStateException(format("Tenant id is not set. Kind of TenantId is %s.",
                                                       kindCase.toString()));
        }
    }

    private static class TenantIdRetriever extends TenantFunction<TenantId> {

        private static TenantIdRetriever instance() {
            return Singleton.INSTANCE.value;
        }

        private TenantIdRetriever() {
            super(true);
        }

        @Override
        public TenantId apply(@Nullable TenantId input) {
            checkNotNull(input);
            return input;
        }

        private enum Singleton {
            INSTANCE;
            @SuppressWarnings("NonSerializableFieldInSerializableClass")
            private final TenantIdRetriever value = new TenantIdRetriever();
        }
    }
}
