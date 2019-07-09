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

import com.google.common.collect.ImmutableMap;
import io.spine.annotation.Internal;
import io.spine.core.TenantId;

import static com.google.common.base.Preconditions.checkState;
import static io.spine.server.storage.datastore.tenant.Namespace.DOMAIN_PREFIX;
import static io.spine.server.storage.datastore.tenant.Namespace.EMAIL_PREFIX;
import static io.spine.server.storage.datastore.tenant.Namespace.STRING_VALUE_PREFIX;

/**
 * A default implementation of {@link NamespaceConverter}.
 */
@Internal
final class DefaultNamespaceConverter extends NamespaceConverter {

    private static final ImmutableMap<String, Namespace.ConverterType> TYPE_PREFIX_TO_CONVERTER =
            ImmutableMap.of(DOMAIN_PREFIX, Namespace.ConverterType.DOMAIN,
                            EMAIL_PREFIX, Namespace.ConverterType.EMAIL,
                            STRING_VALUE_PREFIX, Namespace.ConverterType.VALUE);

    private final boolean multitenant;

    /**
     * Creates an instance of {@code DefaultNamespaceConverter} with a passed multi-tenancy
     * setting.
     *
     * @param multitenant
     *         whether this converter is configured to work in multi-tenant environment
     */
    public DefaultNamespaceConverter(boolean multitenant) {
        this.multitenant = multitenant;
    }

    private static NamespaceConverter forTenant(TenantId id) {
        Namespace.ConverterType converterType =
                Namespace.ConverterType.forTenantId(id, null);
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
            converterType = TYPE_PREFIX_TO_CONVERTER.get(typePrefix);
            checkState(converterType != null,
                       "Could not determine a TenantId converter for namespace %s.",
                       namespace);
        }
        return converterType.converter().toTenantId(namespace);
    }
}
