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

import com.google.cloud.datastore.Key;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.spine3.net.EmailAddress;
import org.spine3.net.InternetDomain;
import org.spine3.server.storage.datastore.ProjectId;
import org.spine3.users.TenantId;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.regex.Matcher.quoteReplacement;
import static org.spine3.server.storage.datastore.tenant.DatastoreTenants.*;

/**
 * A value object representing the Datastore
 * <a href="https://cloud.google.com/datastore/docs/concepts/multitenancy">namespace</a>.
 *
 * <p>The primary usage of the namespaces is multitenancy.
 *
 * <p>A namespace constructed from a {@link TenantId} will have a one capital letter type prefix
 * depending on which field of the {@link TenantId} has the actual value. These prefixes are:
 * <ul>
 *     <li>{@code D} - for "Internet Domain";
 *     <li>{@code E} - for "Email";
 *     <li>{@code V} - for "String Value".
 * </ul>
 *
 * <p>If a {@link NamespaceToTenantIdConverter} is
 * {@linkplain DatastoreTenants#registerNamespaceConverter registered}, then the converter is used
 * and the prefixes are absent.
 *
 * <p>One should register a {@link NamespaceToTenantIdConverter} <b>if and only if</b>
 * the used Datastore already contains namespaces to work with.
 *
 * <p>Please note, that for working with the Datastore namespaces, Spine requires one of the
 * following conditions to be met:
 * <ul>
 *     <li>There are no namespaces in the Datastore at all. All the namespace manipulations are
 *     preformed by the mean of the framework.
 *     <li>All the present namespaces start with one of the prefixes listed above. In this case
 *     the described {@link TenantId} conversion behavior will be applied.
 *     <li>A custom {@link NamespaceToTenantIdConverter} is registered.
 * </ul>
 *
 * <p>If none of the above conditions is met, runtime errors may happen.
 *
 * @author Dmytro Dashenkov
 * @see DatastoreTenants
 * @see NamespaceSupplier
 */
public final class Namespace {

    private static final String DOMAIN_PREFIX = "D";
    private static final String EMAIL_PREFIX = "E";
    private static final String STRING_VALUE_PREFIX = "V";

    private static final ImmutableMap<String, TenantIdConverterType> TYPE_PREFIX_TO_CONVERTER =
            ImmutableMap.of(DOMAIN_PREFIX, TenantIdConverterType.DOMAIN,
                            EMAIL_PREFIX, TenantIdConverterType.EMAIL,
                            STRING_VALUE_PREFIX, TenantIdConverterType.VALUE);

    private static final int SIGNIFICANT_PART_START_INDEX = 1;

    private static final Pattern AT_SYMBOL_PATTERN = Pattern.compile("@", Pattern.LITERAL);
    private static final String AT_SYMBOL_REPLACEMENT = "-at-";

    private final String value;
    private final NamespaceToTenantIdConverter converter;

    private Namespace() {
        this("", TenantIdConverterType.VALUE.namespaceConverter);
    }

    private Namespace(String value,
                      NamespaceToTenantIdConverter customConverter) {
        this.value = escapeIllegalCharacters(value);
        this.converter = customConverter;
    }

    /**
     * Creates new instance of {@code Namespace} from the given string.
     *
     * @param datastoreNamespace a string representing the datastore namespace
     * @return new instance of {@code Namespace}
     */
    static Namespace of(String datastoreNamespace) {
        if (datastoreNamespace.isEmpty()) {
            return new Namespace();
        } else {
            return new Namespace(datastoreNamespace,
                                 TenantIdConverterType.CUSTOM.namespaceConverter);
        }
    }

    /**
     * Creates new instance of {@code Namespace} from the given {@link TenantId}.
     *
     * @param id the {@link TenantId} to create the {@code Namespace} from
     * @return new instance of {@code Namespace}
     */
    static Namespace of(TenantId id, ProjectId projectId) {
        checkNotNull(id);
        checkNotNull(projectId);

        final Optional<NamespaceToTenantIdConverter> customConverter =
                getNamespaceConverter(projectId);

        final TenantIdConverterType tenantIdConverterType =
                TenantIdConverterType.forTenantId(id, customConverter.orNull());
        final NamespaceToTenantIdConverter converter = customConverter
                .or(tenantIdConverterType.namespaceConverter);
        final String ns = converter.toString(id);
        return new Namespace(ns,
                             customConverter.or(tenantIdConverterType.namespaceConverter));
    }

    /**
     * Creates new instance of {@code Namespace} from the name of the given {@link Key}.
     *
     * @param key the {@link Key} to get a {@code Namespace} from
     * @return a {@code Namespace} of the given Key name or {@code null} if the name is
     * {@code null} or empty
     */
    @Nullable
    static Namespace fromNameOf(Key key, ProjectId projectId) {
        checkNotNull(key);
        checkNotNull(projectId);

        final Optional<NamespaceToTenantIdConverter> customConverter =
                getNamespaceConverter(projectId);
        final String namespace = key.getName();
        if (isNullOrEmpty(namespace)) {
            return null;
        }

        final TenantIdConverterType tenantIdConverterType;
        if (customConverter.isPresent()) {
            tenantIdConverterType = TenantIdConverterType.PREDEFINED_VALUE;
        } else {
            final String typePrefix = String.valueOf(namespace.charAt(0));
            tenantIdConverterType = TYPE_PREFIX_TO_CONVERTER.get(typePrefix);
            checkState(tenantIdConverterType != null,
                       "Could not determine a TenantId converter for namespace %s.",
                       namespace);
        }

        final NamespaceToTenantIdConverter defaultConverter = tenantIdConverterType.namespaceConverter;
        final Namespace result = new Namespace(namespace,
                                               customConverter.or(defaultConverter));
        return result;
    }

    private static String escapeIllegalCharacters(String candidateNamespace) {
        final String result = AT_SYMBOL_PATTERN.matcher(candidateNamespace)
                                               .replaceAll(quoteReplacement(AT_SYMBOL_REPLACEMENT));
        return result;
    }

    /**
     * @return a string value of this {@code Namespace}
     */
    public String getValue() {
        return value;
    }

    /**
     * Converts this object to a {@link TenantId}.
     *
     * <p>If current instance was created with
     * {@link Namespace#of(TenantId, ProjectId)}, then the result will be
     * {@code equal} to that {@link TenantId}.
     *
     * <p>If current instance was created with {@link Namespace#of(String)}, then the result will
     * be equivalent to the result of
     * <pre>
     * {@code
     *         TenantId.newBuilder()
     *                 .setValue(namespace.getValue())
     *                 .build();
     * }
     * </pre>
     *
     * @return a {@link TenantId} represented by this {@code Namespace}
     */
    TenantId toTenantId() {
        final TenantId tenantId = converter.convert(getValue());
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
     * An enumeration of converters of the {@code Namespace} into a {@link TenantId} of the specific
     * type.
     */
    private enum TenantIdConverterType {

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which has a {@code domain}.
         */
        DOMAIN(new DomainNamespaceToTenantIdConverter()),
        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which has an {@code email}.
         */
        EMAIL(new EmailNamespaceToTenantIdConverter()),

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which represents a string
         * value.
         */
        VALUE(new StringValueNamespaceToTenantIdConverter()),

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which represents a string
         * value.
         *
         * <p>The difference to {@link #VALUE} is that the namespaces associated with this
         * converter have no type prefix, as they are defined by the user beforehand.
         *
         * <p>This strategy uses a
         * {@link com.google.common.base.Converter Converter&lt;String, TenantId&gt;} if it is
         * {@linkplain DatastoreTenants#getNamespaceConverter registered}, or throws an exception
         * if the it's not.
         */
        PREDEFINED_VALUE(new StubNamespaceToTenantIdConverter()),

        /**
         * Represents a custom user-defined namespace for a single-tenant storage.
         *
         * The conversion performed by the converter of this object acts with the string value of
         * a {@link TenantId} and performs no action on the namespace string itself.
         */
        CUSTOM(new EmptyNamespaceConverter());

        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        // This enum is ancillary and is not to be serialized
        private final NamespaceToTenantIdConverter namespaceConverter;

        private static TenantIdConverterType forTenantId(
                TenantId tenantId,
                @Nullable NamespaceToTenantIdConverter customConverter) {
            if (customConverter != null) {
                return PREDEFINED_VALUE;
            }
            final TenantId.KindCase kindCase = tenantId.getKindCase();
            final String kindCaseName = kindCase.name();
            final TenantIdConverterType converter = valueOf(kindCaseName);
            return converter;
        }

        TenantIdConverterType(NamespaceToTenantIdConverter namespaceConverter) {
            this.namespaceConverter = namespaceConverter;
        }
    }

    /**
     * A converter for the framework-defined namespaces, which are stored with a type prefix.
     */
    abstract static class PrefixedNamespaceToTenantIddConverter
            extends NamespaceToTenantIdConverter {

        private final String prefix;

        PrefixedNamespaceToTenantIddConverter(String prefix) {
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

    private static class EmptyNamespaceConverter extends NamespaceToTenantIdConverter {

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
