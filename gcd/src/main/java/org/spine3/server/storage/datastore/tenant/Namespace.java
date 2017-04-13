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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import org.spine3.net.EmailAddress;
import org.spine3.net.InternetDomain;
import org.spine3.server.storage.datastore.DatastoreStorageFactory;
import org.spine3.users.TenantId;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * A value object representing the Datastore
 * <a href="https://cloud.google.com/datastore/docs/concepts/multitenancy">namespace</a>.
 *
 * <p>The primary usage of the namespaces is multitenancy.
 *
 * @author Dmytro Dashenkov
 * @see NamespaceSupplier
 */
public final class Namespace {

    private static final char TENANT_ID_DOMAIN_PREFIX = 'D';
    private static final char TENANT_ID_EMAIL_PREFIX = 'E';
    private static final char TENANT_ID_VALUE_PREFIX = 'V';

    private static final int SIGNIFICANT_NAMESPACE_PART_START_INDEX = 1;
    private static final int PREFIX_INDEX = 0;

    private static final Pattern AT_SYMBOL_PATTERN = Pattern.compile("@", Pattern.LITERAL);
    private static final String AT_SYMBOL_REPLACEMENT = "-at-";

    private final String value;

    private Namespace(String value) {
        this.value = value;
    }

    /**
     * Creates new instance of {@code Namespace} from the given string.
     *
     * @param datastoreNamespace a string representing the datastore namespace
     * @return new instance of {@code Namespace}
     */
    static Namespace of(String datastoreNamespace) {
        return new Namespace(
                escapeIllegalCharacters(datastoreNamespace));
    }

    /**
     * Creates new instance of {@code Namespace} from the given {@link TenantId}.
     *
     * @param tenantId the {@link TenantId} to create the {@code Namespace} from
     * @return new instance of {@code Namespace}
     */
    static Namespace of(TenantId tenantId) {
        final String idStringValue = tenantIdToSignificantString(tenantId);
        return of(idStringValue);
    }

    /**
     * @return a human-friendly string representation of the {@link TenantId}
     */
    private static String tenantIdToSignificantString(TenantId id) {
        final TenantId.KindCase kindCase = id.getKindCase();
        final String result;
        switch (kindCase) {
            case DOMAIN:
                result = TENANT_ID_DOMAIN_PREFIX + id.getDomain()
                                                     .getValue();

                break;
            case EMAIL:
                result = TENANT_ID_EMAIL_PREFIX + id.getEmail()
                                                    .getValue();
                break;
            case VALUE:
                result = TENANT_ID_VALUE_PREFIX + id.getValue();
                break;
            case KIND_NOT_SET:
            default:
                throw new IllegalArgumentException(format("Tenant ID is not set. Kind of TenantId is %s.",
                                                          kindCase.toString()));
        }
        return result;
    }

    private static String escapeIllegalCharacters(String candidateNamespace) {
        final String result = AT_SYMBOL_PATTERN.matcher(candidateNamespace)
                                               .replaceAll(Matcher.quoteReplacement(AT_SYMBOL_REPLACEMENT));
        return result;
    }

    /**
     * @return a string value of this {@code Namespace}
     */
    public String getValue() {
        final String result = value.substring(SIGNIFICANT_NAMESPACE_PART_START_INDEX);
        return result;
    }

    TenantId toTenantId() throws IllegalStateException {
        final char prefix = value.charAt(PREFIX_INDEX);
        final TenantId tenantId;
        switch (prefix) {
            case TENANT_ID_DOMAIN_PREFIX:
                final InternetDomain domain = InternetDomain.newBuilder()
                                                            .setValue(getValue())
                                                            .build();
                tenantId = TenantId.newBuilder()
                                   .setDomain(domain)
                                   .build();
                break;
            case TENANT_ID_EMAIL_PREFIX:
                final EmailAddress email = EmailAddress.newBuilder()
                                                       .setValue(getValue())
                                                       .build();
                tenantId = TenantId.newBuilder()
                                   .setEmail(email)
                                   .build();
                break;
            case TENANT_ID_VALUE_PREFIX:
                tenantId = TenantId.newBuilder()
                                   .setValue(getValue())
                                   .build();
                break;
            default:
                throw new IllegalStateException(format("Malformed Namespace \"%s\"", value));
        }
        return tenantId;
    }

    @Override
    public String toString() {
        return getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Namespace namespace = (Namespace) o;
        return Objects.equal(getValue(), namespace.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getValue());
    }

    /**
     * A supplier for the {@linkplain Namespace namespaces}, based on the current multitenancy configuration and
     * {@linkplain TenantId tenant ID}.
     *
     * @author Dmytro Dashenkov
     */
    abstract static class NamespaceSupplier implements Supplier<Namespace> {

        /**
         * Obtains an instance of {@code NamespaceSupplier} for the passed
         * {@linkplain DatastoreStorageFactory storage factory}.
         *
         * @param factory the {@linkplain DatastoreStorageFactory storage factory} to return a supplier for
         * @see org.spine3.server.storage.StorageFactory#isMultitenant
         */
        static NamespaceSupplier instanceFor(DatastoreStorageFactory factory) {
            checkNotNull(factory);
            if (factory.isMultitenant()) {
                return Singleton.INSTANCE.multipleTenant;
            } else {
                return Singleton.INSTANCE.singleTenant;
            }
        }

        @VisibleForTesting
        static NamespaceSupplier constant() {
            return Singleton.INSTANCE.singleTenant;
        }

        @VisibleForTesting
        static NamespaceSupplier multitenant() {
            return Singleton.INSTANCE.multipleTenant;
        }

        /**
         * Generates a {@link Namespace} based on the current {@linkplain TenantId tenant ID}.
         *
         * @return an instance of {@link Namespace} representing either the current tenant ID or an empty string if
         * the {@linkplain DatastoreStorageFactory storage factory} passed upon the initialization is configured to be
         * single tenant
         */
        @Override
        public abstract Namespace get();

        private enum Singleton {
            INSTANCE;
            @SuppressWarnings("NonSerializableFieldInSerializableClass")
            private final NamespaceSupplier singleTenant = new SingleTenantNamespaceSupplier();
            @SuppressWarnings("NonSerializableFieldInSerializableClass")
            private final NamespaceSupplier multipleTenant = new MultitenantNamespaceSupplier();
        }
    }
}
