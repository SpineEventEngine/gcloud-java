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
import io.spine.net.EmailAddress;
import io.spine.net.InternetDomain;

import static io.spine.server.storage.datastore.tenant.Namespace.DOMAIN_PREFIX;
import static io.spine.server.storage.datastore.tenant.Namespace.EMAIL_PREFIX;
import static io.spine.server.storage.datastore.tenant.Namespace.STRING_VALUE_PREFIX;

/**
 * A factory of {@link NamespaceConverter} instances for the particular purposes.
 */
final class NamespaceConverters {

    /**
     * Prevents the utility class instantiation.
     */
    private NamespaceConverters() {
    }

    /**
     * Creates a converter for a namespace and a {@link TenantId} which has the {@code domain}
     * field.
     *
     * <p>Appends the {@link Namespace#DOMAIN_PREFIX "D"} prefix to the result namespace string.
     *
     * @return new instance of the domain-based {@link TenantId} converter
     */
    static NamespaceConverter forDomain() {
        return new DomainNamespaceConverter();
    }

    /**
     * Creates a converter for a namespace and a {@link TenantId} which has the {@code email}
     * field.
     *
     * <p>Appends the {@link Namespace#EMAIL_PREFIX "E"} prefix to the result namespace string.
     *
     * @return new instance of the email-based {@link TenantId} converter
     */
    static NamespaceConverter forEmail() {
        return new EmailNamespaceConverter();
    }

    /**
     * Creates a converter for a namespace and a {@link TenantId} which has the string {@code value}
     * field.
     *
     * <p>Appends the {@link Namespace#STRING_VALUE_PREFIX "V"} prefix to the result namespace
     * string.
     *
     * @return new instance of the string-based {@link TenantId} converter
     */
    static NamespaceConverter forStringValue() {
        return new StringValueNamespaceConverter();
    }

    /**
     * Creates a converter for a custom user-defined namespace, which appears in a single-tenant
     * storage and a {@link TenantId} which has the string {@code value} field.
     *
     * <p>The difference to the {@link #forStringValue() NamespaceConverters.forStringValue()} is
     * that this implementation does not append any type prefix to the result namespace string.
     *
     * @return new instance of the domain-based {@link TenantId} converter
     */
    static NamespaceConverter forCustomNamespace() {
        return new CustomNamespaceConverter();
    }

    /**
     * A converter for the framework-defined namespaces, which are stored with a type prefix.
     */
    abstract static class PrefixedNamespaceToTenantIdConverter extends NamespaceConverter {

        private final String prefix;

        PrefixedNamespaceToTenantIdConverter(String prefix) {
            super();
            this.prefix = prefix;
        }

        @Override
        protected String toString(TenantId tenantId) {
            return prefix + toSignificantString(tenantId);
        }

        @Override
        protected final TenantId toTenantId(String namespace) {
            if (namespace.startsWith(prefix) && namespace.length() > prefix.length()) {
                var significantNamespacePart = namespace.substring(prefix.length());
                return significantStringToTenantId(significantNamespacePart);
            } else {
                return NOT_A_TENANT;
            }
        }

        abstract TenantId significantStringToTenantId(String namespace);

        abstract String toSignificantString(TenantId tenantId);
    }

    private static class DomainNamespaceConverter extends PrefixedNamespaceToTenantIdConverter {

        private DomainNamespaceConverter() {
            super(DOMAIN_PREFIX);
        }

        @Override
        protected String toSignificantString(TenantId tenantId) {
            var value = tenantId.getDomain()
                                .getValue();
            return value;
        }

        @Override
        protected TenantId significantStringToTenantId(String namespace) {
            var domain = InternetDomain.newBuilder()
                    .setValue(namespace)
                    .build();
            var tenantId = TenantId.newBuilder()
                    .setDomain(domain)
                    .build();
            return tenantId;
        }
    }

    private static class EmailNamespaceConverter
            extends PrefixedNamespaceToTenantIdConverter {

        private EmailNamespaceConverter() {
            super(EMAIL_PREFIX);
        }

        @Override
        protected String toSignificantString(TenantId tenantId) {
            var value = tenantId.getEmail()
                                .getValue();
            return value;
        }

        @Override
        protected TenantId significantStringToTenantId(String namespace) {
            var email = EmailAddress.newBuilder()
                    .setValue(namespace)
                    .build();
            var tenantId = TenantId.newBuilder()
                    .setEmail(email)
                    .build();
            return tenantId;
        }
    }

    private static class StringValueNamespaceConverter
            extends PrefixedNamespaceToTenantIdConverter {

        private StringValueNamespaceConverter() {
            super(STRING_VALUE_PREFIX);
        }

        @Override
        protected String toSignificantString(TenantId tenantId) {
            var value = tenantId.getValue();
            return value;
        }

        @Override
        protected TenantId significantStringToTenantId(String namespace) {
            var tenantId = TenantId.newBuilder()
                    .setValue(namespace)
                    .build();
            return tenantId;
        }
    }

    private static class CustomNamespaceConverter extends NamespaceConverter {

        @Override
        protected String toString(TenantId tenantId) {
            var ns = tenantId.getValue();
            return ns;
        }

        @Override
        protected TenantId toTenantId(String namespace) {
            var tenantId = TenantId.newBuilder()
                    .setValue(namespace)
                    .build();
            return tenantId;
        }
    }
}
