/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import io.spine.annotation.Internal;
import io.spine.core.TenantId;

import static com.google.common.base.Preconditions.checkState;

/**
 * A default implementation of {@link NamespaceConverter}.
 */
@Internal
final class DefaultNamespaceConverter extends NamespaceConverter {

    private final boolean multitenant;

    /**
     * Creates an instance of {@code DefaultNamespaceConverter} with the passed multi-tenancy
     * setting.
     *
     * @param multitenant
     *         whether this converter is configured to work in multi-tenant environment
     */
    DefaultNamespaceConverter(boolean multitenant) {
        super();
        this.multitenant = multitenant;
    }

    private static NamespaceConverter forTenant(TenantId id) {
        Namespace.ConverterType converterType = Namespace.ConverterType.forTenantId(id);
        return converterType.converter();
    }

    @Override
    protected String toString(TenantId tenantId) {
        return forTenant(tenantId).toString(tenantId);
    }

    @Override
    protected TenantId toTenantId(String namespace) {
        Namespace.ConverterType converterType;
        if (!multitenant) {
            converterType = Namespace.ConverterType.SINGLE_CUSTOM;
        } else {
            String typePrefix = String.valueOf(namespace.charAt(0));
            converterType = Namespace.typeOfPrefix(typePrefix);
            checkState(converterType != null,
                       "Could not determine a `TenantId` converter for namespace %s.",
                       namespace);
        }
        return converterType.converter().toTenantId(namespace);
    }
}
