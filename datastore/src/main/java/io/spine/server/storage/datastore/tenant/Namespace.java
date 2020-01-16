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

import com.google.cloud.datastore.Key;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import io.spine.core.TenantId;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.concurrent.Immutable;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.regex.Matcher.quoteReplacement;

/**
 * A value object representing the Datastore
 * <a href="https://cloud.google.com/datastore/docs/concepts/multitenancy">namespace</a>.
 *
 * <p>The primary usage of the namespaces is multitenancy.
 *
 * <p>By default, a namespace constructed from a {@link TenantId} has a single capital letter
 * prefix reflecting the type of {@link TenantId}. These prefixes are:
 * <ul>
 *      <li>{@code D} - for "Internet Domain";
 *      <li>{@code E} - for "Email";
 *      <li>{@code V} - for "String Value".
 * </ul>
 *
 * <p>The framework users may override the behavior by
 * {@link io.spine.server.storage.datastore.DatastoreStorageFactory.Builder#setNamespaceConverter(NamespaceConverter)
 * registering} a custom {@link NamespaceConverter}. If registered, no defaults are applied.
 *
 * <p>One should register a {@link NamespaceConverter} <b>if and only if</b>
 * the target Datastore instance already contains namespaces to work with.
 *
 * <p>Basically, one of three scenarios in working with the Datastore namespaces takes place.
 * <ul>
 *      <li>There are no namespaces in the Datastore at all. All the namespace manipulations are
 *      performed by the means of the framework using the defaults.
 *
 *      <li>All the present namespaces start with one of the prefixes listed above. In this case
 *      the described default {@code TenantId} conversion behavior is applied.
 *
 *      <li>A custom {@link NamespaceConverter} is registered, and its implementation is consistent
 * with the namespaces (if any) already present in the Datastore instance.
 * </ul>
 *
 * @see DatastoreTenants
 * @see NamespaceSupplier
 */
@Immutable
public final class Namespace {

    static final String DOMAIN_PREFIX = "D";
    static final String EMAIL_PREFIX = "E";
    static final String STRING_VALUE_PREFIX = "V";

    private static final ImmutableMap<String, Namespace.ConverterType> TYPE_PREFIX_TO_CONVERTER =
            ImmutableMap.of(DOMAIN_PREFIX, Namespace.ConverterType.DOMAIN,
                            EMAIL_PREFIX, Namespace.ConverterType.EMAIL,
                            STRING_VALUE_PREFIX, Namespace.ConverterType.VALUE);

    private static final Pattern AT_SYMBOL_PATTERN = Pattern.compile("@", Pattern.LITERAL);
    private static final String AT_SYMBOL_REPLACEMENT = "-at-";

    private final String value;
    private final NamespaceConverter converter;

    private Namespace() {
        this("", ConverterType.SINGLE_CUSTOM.namespaceConverter);
    }

    private Namespace(String value, NamespaceConverter customConverter) {
        this.value = escapeIllegalCharacters(value);
        this.converter = customConverter;
    }

    /**
     * Creates new instance of {@code Namespace} from the given string.
     *
     * @param datastoreNamespace
     *         a string representing the datastore namespace
     * @return new instance of {@code Namespace}
     */
    static Namespace of(String datastoreNamespace) {
        if (datastoreNamespace.isEmpty()) {
            return new Namespace();
        } else {
            return new Namespace(datastoreNamespace,
                                 ConverterType.SINGLE_CUSTOM.namespaceConverter);
        }
    }

    /**
     * Creates new instance of {@code Namespace} from the given {@link TenantId}.
     *
     * @param id
     *         the {@link TenantId} to create the {@code Namespace} from
     * @param multitenant
     *         whether the application is multi-tenant
     * @param converterFactory
     *         the converter factory to use
     * @return new instance of {@code Namespace}
     */
    static Namespace of(TenantId id, boolean multitenant, NsConverterFactory converterFactory) {
        checkNotNull(id);
        checkNotNull(converterFactory);

        NamespaceConverter converter = converterFactory.get(multitenant);
        String ns = converter.toString(id);
        return new Namespace(ns, converter);
    }

    /**
     * Creates new instance of {@code Namespace} from the given {@link TenantId}.
     *
     * <p>Similar to {@link #of(TenantId, boolean, NsConverterFactory)}, but the
     * {@link NsConverterFactory#defaults() the default} converter factory is used.
     *
     * @param id
     *         the {@link TenantId} to create the {@code Namespace} from
     * @param multitenant
     *         whether the application is multi-tenant
     * @return new instance of {@code Namespace}
     */
    static Namespace of(TenantId id, boolean multitenant) {
        return of(id, multitenant, NsConverterFactory.defaults());
    }

    /**
     * Creates new instance of {@code Namespace} from the name of the given {@link Key}.
     *
     * @param key
     *         the {@link Key} to get a {@code Namespace} from
     * @param multitenant
     *         whether the storage is multi-tenant
     * @param converterFactory
     *         the converter factory to use
     * @return a {@code Namespace} of the given Key name or {@code null} if the name is
     *         {@code null} or empty
     */
    static @Nullable Namespace fromNameOf(Key key,
                                          boolean multitenant,
                                          NsConverterFactory converterFactory) {
        checkNotNull(key);
        checkNotNull(converterFactory);

        String namespace = key.getName();
        if (isNullOrEmpty(namespace)) {
            return null;
        }

        NamespaceConverter converter = converterFactory.get(multitenant);
        Namespace result = new Namespace(namespace, converter);
        return result;
    }

    /**
     * Creates new instance of {@code Namespace} from the name of the given {@link Key}.
     *
     * <p>Uses the {@link NsConverterFactory#defaults() default} converter factory.
     *
     * @param key
     *         the {@link Key} to get a {@code Namespace} from
     * @param multitenant
     *         whether the storage is multi-tenant
     * @return a {@code Namespace} of the given Key name or {@code null} if the name is
     *         {@code null} or empty
     * @see #fromNameOf(Key, boolean, NsConverterFactory)
     */
    static @Nullable Namespace fromNameOf(Key key, boolean multitenant) {
        return fromNameOf(key, multitenant, NsConverterFactory.defaults());
    }

    /**
     * Obtains a {@code ConverterType} for the given prefix.
     *
     * @param prefix
     *         the prefix value
     * @return the {@code ConverterType} instance, or {@code null} if there is no type configured
     *         for the prefix.
     */
    static @Nullable ConverterType typeOfPrefix(String prefix) {
        checkNotNull(prefix);
        return TYPE_PREFIX_TO_CONVERTER.get(prefix);
    }

    private static String escapeIllegalCharacters(String candidateNamespace) {
        String result = AT_SYMBOL_PATTERN.matcher(candidateNamespace)
                                         .replaceAll(quoteReplacement(AT_SYMBOL_REPLACEMENT));
        return result;
    }

    /**
     * Obtains a string value of this {@code Namespace}.
     */
    public String getValue() {
        return value;
    }

    /**
     * Converts this object to a {@link TenantId}.
     *
     * <p>If current instance was created with
     * {@link Namespace#of(TenantId, boolean, NsConverterFactory)}, then the result will be
     * {@code equal} to that {@link TenantId}.
     *
     * <p>If current instance was created with {@link Namespace#of(String)},
     * then the result will be equivalent to the result of
     * <pre>
     *     {@code
     *     TenantId.newBuilder()
     *             .setValue(namespace.getValue())
     *             .vBuild();
     *     }
     * </pre>
     *
     * @return a {@link TenantId} represented by this {@code Namespace}
     */
    TenantId toTenantId() {
        TenantId tenantId = converter.convert(getValue());
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
    enum ConverterType {

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which has a {@code domain}.
         */
        DOMAIN(NamespaceConverters.forDomain()),

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which has an {@code email}.
         */
        EMAIL(NamespaceConverters.forEmail()),

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which represents a string
         * value.
         */
        VALUE(NamespaceConverters.forStringValue()),

        /**
         * Represents a custom user-defined namespace for a single-tenant storage.
         *
         * The conversion performed by the converter of this object acts with the string value of
         * a {@link TenantId} and performs no action on the namespace string itself.
         */
        SINGLE_CUSTOM(NamespaceConverters.forCustomNamespace());

        private final NamespaceConverter namespaceConverter;

        static ConverterType forTenantId(TenantId tenantId) {
            TenantId.KindCase kindCase = tenantId.getKindCase();
            String kindCaseName = kindCase.name();
            ConverterType converter = valueOf(kindCaseName);
            return converter;
        }

        ConverterType(NamespaceConverter namespaceConverter) {
            this.namespaceConverter = namespaceConverter;
        }

        NamespaceConverter converter() {
            return namespaceConverter;
        }}
}
