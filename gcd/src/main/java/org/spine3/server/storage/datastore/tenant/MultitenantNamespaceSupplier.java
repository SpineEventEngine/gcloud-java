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

import org.spine3.server.storage.datastore.ProjectId;
import org.spine3.server.tenant.TenantFunction;
import org.spine3.users.TenantId;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link NamespaceSupplier} for multitenant storage factories.
 *
 * @author Dmytro Dashenkov
 */
final class MultitenantNamespaceSupplier extends NamespaceSupplier {

    private final ProjectId projectId;

    static NamespaceSupplier forProject(ProjectId projectId) {
        return new MultitenantNamespaceSupplier(projectId);
    }

    private MultitenantNamespaceSupplier(ProjectId projectId) {
        super();
        this.projectId = checkNotNull(projectId);
    }

    /**
     * {@inheritDoc}
     *
     * @return the {@link Namespace} representing the current tenant {@link TenantId ID}
     */
    @Override
    public Namespace get() {
        final TenantIdRetriever retriever = new TenantIdRetriever();
        final TenantId tenantId = retriever.execute();
        checkNotNull(tenantId);
        final Namespace result = Namespace.of(tenantId, projectId);
        return result;
    }

    /**
     * A function declosuring the current tenant {@linkplain TenantId ID}.
     *
     */
    private static class TenantIdRetriever extends TenantFunction<TenantId> {

        /**
         * Creates a new instance of {@code TenantIdRetriever}.
         *
         * @throws IllegalStateException if the application is has a single tenant
         */
        private TenantIdRetriever() throws IllegalStateException {
            super(true);
        }

        /**
         * Retrieves the passed {@link TenantId}, ensuring it's not equal to {@code null}.
         *
         * @param input current {@link TenantId}
         * @return the input
         */
        @Override
        public TenantId apply(@Nullable TenantId input) {
            checkNotNull(input);
            return input;
        }
    }
}
