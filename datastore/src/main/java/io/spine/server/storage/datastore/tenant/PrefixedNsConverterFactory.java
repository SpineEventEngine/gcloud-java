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
import io.spine.server.storage.datastore.tenant.NamespaceConverters.PrefixedNamespaceToTenantIdConverter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link NsConverterFactory} which creates converters which work with prefixed namespaces.
 *
 * <p>Users may want to supply custom namespaces for multitenant systems. In such case,
 * the resulting namespace which contains the data is composed of the given namespace (prefix) and
 * the tenant ID string representation. The two parts are separated with a {@code .} (dot) symbol.
 */
@Internal
public final class PrefixedNsConverterFactory implements NsConverterFactory {

    private static final String SEPARATOR = ".";

    private final String namespacePrefix;
    private final NsConverterFactory delegate;

    public PrefixedNsConverterFactory(String namespacePrefix, NsConverterFactory delegate) {
        this.namespacePrefix = checkNotNull(namespacePrefix);
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public NamespaceConverter get(boolean multitenant) {
        NamespaceConverter converter = delegate.get(multitenant);
        return new Converter(namespacePrefix, converter);
    }

    /**
     * A {@link NamespaceConverter} which processes the namespace prefix and delegates
     * the significant part of a namespace to another converter.
     */
    private static final class Converter extends PrefixedNamespaceToTenantIdConverter {

        private final NamespaceConverter delegate;

        private Converter(String prefix, NamespaceConverter delegate) {
            super(prefix);
            this.delegate = checkNotNull(delegate);
        }

        @Override
        TenantId significantStringToTenantId(String namespace) {
            return namespace.startsWith(SEPARATOR)
                   ? delegate.convert(namespace.substring(SEPARATOR.length()))
                   : NOT_A_TENANT;
        }

        @Override
        String toSignificantString(TenantId tenantId) {
            return SEPARATOR + delegate.reverse().convert(tenantId);
        }
    }
}
