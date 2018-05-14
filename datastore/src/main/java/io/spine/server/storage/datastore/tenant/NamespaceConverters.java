/*
 * Copyright 2018, TeamDev. All rights reserved.
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
 * A factory of {@link NamespaceToTenantIdConverter} instances for the particular purposes.
 *
 * @author Dmytro Dashenkov
 */
final class NamespaceConverters {

    private static final int SIGNIFICANT_PART_START_INDEX = 1;

    private NamespaceConverters() {
        // Prevent static class initialization
    }

    /**
     * Creates a converter for a namespace and a {@link TenantId} which has the {@code domain}
     * field.
     *
     * <p>Appends the {@link Namespace#DOMAIN_PREFIX "D"} prefix to the result namespace string.
     *
     * @return new instance of the domain-based {@link TenantId} converter
     */
    static NamespaceToTenantIdConverter forDomain() {
        return new DomainNamespaceToTenantIdConverter();
    }

    /**
     * Creates a converter for a namespace and a {@link TenantId} which has the {@code email}
     * field.
     *
     * <p>Appends the {@link Namespace#EMAIL_PREFIX "E"} prefix to the result namespace string.
     *
     * @return new instance of the email-based {@link TenantId} converter
     */
    static NamespaceToTenantIdConverter forEmail() {
        return new EmailNamespaceToTenantIdConverter();
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
    static NamespaceToTenantIdConverter forStringValue() {
        return new StringValueNamespaceToTenantIdConverter();
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
    static NamespaceToTenantIdConverter forCustomNamespace() {
        return new CustomNamespaceConverter();
    }

    /**
     * Creates a stub {@link NamespaceToTenantIdConverter}.
     *
     * <p>This implementation throws {@link UnsupportedOperationException} on any operation.
     *
     * <p>The result of this method should be used if there is a custom user-defined
     * {@link NamespaceToTenantIdConverter converter} which should be used instead.
     *
     * @return a stub instance of {@link NamespaceToTenantIdConverter}
     */
    static NamespaceToTenantIdConverter stub() {
        return new StubNamespaceToTenantIdConverter();
    }

    /**
     * A converter for the framework-defined namespaces, which are stored with a type prefix.
     */
    abstract static class PrefixedNamespaceToTenantIddConverter
            extends NamespaceToTenantIdConverter {

        private final String prefix;

        PrefixedNamespaceToTenantIddConverter(String prefix) {
            super();
            this.prefix = prefix;
        }

        @Override
        protected String toString(TenantId tenantId) {
            return prefix + toSignificantString(tenantId);
        }

        @Override
        protected final TenantId toTenantId(String namespace) {
            final String significantNamespacePart =
                    namespace.substring(SIGNIFICANT_PART_START_INDEX);
            return significantStringToTenantId(significantNamespacePart);
        }

        abstract TenantId significantStringToTenantId(String namespace);

        abstract String toSignificantString(TenantId tenantId);
    }

    private static class DomainNamespaceToTenantIdConverter
            extends PrefixedNamespaceToTenantIddConverter {

        private DomainNamespaceToTenantIdConverter() {
            super(DOMAIN_PREFIX);
        }

        @Override
        protected String toSignificantString(TenantId tenantId) {
            final String value = tenantId.getDomain()
                                         .getValue();
            return value;
        }

        @Override
        protected TenantId significantStringToTenantId(String namespace) {
            final InternetDomain domain = InternetDomain.newBuilder()
                                                        .setValue(namespace)
                                                        .build();
            final TenantId tenantId = TenantId.newBuilder()
                                              .setDomain(domain)
                                              .build();
            return tenantId;
        }
    }

    private static class EmailNamespaceToTenantIdConverter
            extends PrefixedNamespaceToTenantIddConverter {

        private EmailNamespaceToTenantIdConverter() {
            super(EMAIL_PREFIX);
        }

        @Override
        protected String toSignificantString(TenantId tenantId) {
            final String value = tenantId.getEmail()
                                         .getValue();
            return value;
        }

        @Override
        protected TenantId significantStringToTenantId(String namespace) {
            final EmailAddress email = EmailAddress.newBuilder()
                                                   .setValue(namespace)
                                                   .build();
            final TenantId tenantId = TenantId.newBuilder()
                                              .setEmail(email)
                                              .build();
            return tenantId;
        }
    }

    private static class StringValueNamespaceToTenantIdConverter
            extends PrefixedNamespaceToTenantIddConverter {

        private StringValueNamespaceToTenantIdConverter() {
            super(STRING_VALUE_PREFIX);
        }

        @Override
        protected String toSignificantString(TenantId tenantId) {
            final String value = tenantId.getValue();
            return value;
        }

        @Override
        protected TenantId significantStringToTenantId(String namespace) {
            final TenantId tenantId = TenantId.newBuilder()
                                              .setValue(namespace)
                                              .build();
            return tenantId;
        }
    }

    private static class StubNamespaceToTenantIdConverter extends NamespaceToTenantIdConverter {
        @Override
        protected String toString(TenantId tenantId) {
            throw stubUsage();
        }

        @Override
        protected TenantId toTenantId(String namespace) {
            throw stubUsage();
        }

        private static UnsupportedOperationException stubUsage() {
            throw new UnsupportedOperationException("Use custom converter instead.");
        }
    }

    private static class CustomNamespaceConverter extends NamespaceToTenantIdConverter {

        @Override
        protected String toString(TenantId tenantId) {
            final String ns = tenantId.getValue();
            return ns;
        }

        @Override
        protected TenantId toTenantId(String namespace) {
            final TenantId tenantId = TenantId.newBuilder()
                                              .setValue(namespace)
                                              .build();
            return tenantId;
        }
    }
}
