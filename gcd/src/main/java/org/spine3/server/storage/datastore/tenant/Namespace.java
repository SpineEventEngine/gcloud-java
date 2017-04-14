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
import com.google.common.collect.ImmutableMap;
import org.spine3.net.EmailAddress;
import org.spine3.net.InternetDomain;
import org.spine3.users.TenantId;

import javax.annotation.Nullable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

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

    private static final char DOMAIN_PREFIX = 'D';
    private static final char EMAIL_PREFIX = 'E';
    private static final char STRING_VALUE_PREFIX = 'V';

    private static final ImmutableMap<Character, TenantIdConverter> TYPE_PREFIX_TO_CONVERTER =
            ImmutableMap.of(DOMAIN_PREFIX, TenantIdConverter.DOMAIN,
                            EMAIL_PREFIX, TenantIdConverter.EMAIL,
                            STRING_VALUE_PREFIX, TenantIdConverter.VALUE);

    private static final int SIGNIFICANT_PART_START_INDEX = 1;

    private static final Pattern AT_SYMBOL_PATTERN = Pattern.compile("@", Pattern.LITERAL);
    private static final String AT_SYMBOL_REPLACEMENT = "-at-";

    private final String value;
    private final TenantIdConverter tenantIdConverter;

    private Namespace() {
        this("", TenantIdConverter.VALUE);
    }

    private Namespace(String value, TenantIdConverter tenantIdConverter) {
        this.value = escapeIllegalCharacters(value);
        this.tenantIdConverter = tenantIdConverter;
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
            return TenantIdConverter.VALUE.fromSignificantPart(datastoreNamespace);
        }
    }

    /**
     * Creates new instance of {@code Namespace} from the given {@link TenantId}.
     *
     * @param id the {@link TenantId} to create the {@code Namespace} from
     * @return new instance of {@code Namespace}
     */
    static Namespace of(TenantId id) {
        checkNotNull(id);

        final TenantIdConverter converter = TenantIdConverter.forTenantId(id);
        return converter.toNamespace(id);
    }

    /**
     * Creates new instance of {@code Namespace} from the name of the given {@link Key}.
     *
     * @param key the {@link Key} to get a {@code Namespace} from
     * @return a {@code Namespace} of the given Key name or {@code null} if the name is
     * {@code null} or empty
     */
    @Nullable
    static Namespace fromNameOf(Key key) {
        checkNotNull(key);
        final String namespace = key.getName();
        if (isNullOrEmpty(namespace)) {
            return null;
        }

        final char typePrefix = namespace.charAt(0);
        TenantIdConverter tenantIdConverter = TYPE_PREFIX_TO_CONVERTER.get(typePrefix);
        if (tenantIdConverter == null) {
            tenantIdConverter = TenantIdConverter.VALUE;
        }
        final Namespace result = new Namespace(namespace, tenantIdConverter);
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
        return value;
    }

    /**
     * @return a string value of this {@code Namespace} without a type prefix
     */
    private String getSignificantPart() {
        return value.substring(SIGNIFICANT_PART_START_INDEX);
    }

    /**
     * Converts this object to a {@link TenantId}.
     *
     * <p>If current instance was created with {@link Namespace#of(TenantId)}, then the result will
     * be {@code equal} to that {@link TenantId}.
     *
     * <p>If current instance was created with {@link Namespace#of(String)}, then the result will
     * be equivalent to the result of
     * <code>
     * <pre>
     *         TenantId.newBuilder()
     *                 .setValue(namespace.getValue())
     *                 .build();
     *     </pre>
     * </code>
     *
     * @return a {@link TenantId} represented by this {@code Namespace}
     */
    TenantId toTenantId() {
        final TenantId tenantId = tenantIdConverter.toTenantId(this);
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
     * The converter of the {@code Namespace} into a {@link TenantId} of the specific type.
     *
     * <p>By th naming this type is the same as {@link TenantId.KindCase}.
     */
    private enum TenantIdConverter {

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which has a {@code domain}.
         */
        DOMAIN {
            @Override
            TenantId toTenantId(Namespace namespace) {
                final InternetDomain domain = InternetDomain.newBuilder()
                                                            .setValue(namespace.getSignificantPart())
                                                            .build();
                final TenantId tenantId = TenantId.newBuilder()
                                                  .setDomain(domain)
                                                  .build();
                return tenantId;
            }

            @Override
            Namespace toNamespace(TenantId tenantId) {
                final String value = tenantId.getDomain()
                                             .getValue();
                return fromSignificantPart(value);
            }

            @Override
            char getPrefix() {
                return DOMAIN_PREFIX;
            }
        },

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which has an {@code email}.
         */
        EMAIL {
            @Override
            TenantId toTenantId(Namespace namespace) {
                final EmailAddress email = EmailAddress.newBuilder()
                                                       .setValue(namespace.getSignificantPart())
                                                       .build();
                final TenantId tenantId = TenantId.newBuilder()
                                                  .setEmail(email)
                                                  .build();
                return tenantId;
            }

            @Override
            Namespace toNamespace(TenantId tenantId) {
                final String value = tenantId.getEmail()
                                       .getValue();
                return fromSignificantPart(value);
            }

            @Override
            char getPrefix() {
                return EMAIL_PREFIX;
            }
        },

        /**
         * Converts the given {@code Namespace} into a {@link TenantId} which represents a string
         * value.
         */
        VALUE {
            @Override
            TenantId toTenantId(Namespace namespace) {
                final TenantId tenantId = TenantId.newBuilder()
                                                  .setValue(namespace.getSignificantPart())
                                                  .build();
                return tenantId;
            }

            @Override
            Namespace toNamespace(TenantId tenantId) {
                final String value = tenantId.getValue();
                return fromSignificantPart(value);
            }

            @Override
            char getPrefix() {
                return STRING_VALUE_PREFIX;
            }
        };

        private static TenantIdConverter forTenantId(TenantId tenantId) {
            checkNotNull(tenantId);
            final TenantId.KindCase kindCase = tenantId.getKindCase();
            final String kindCaseName = kindCase.name();
            final TenantIdConverter converter = valueOf(kindCaseName);
            return converter;
        }

        abstract TenantId toTenantId(Namespace namespace);

        abstract Namespace toNamespace(TenantId tenantId);

        Namespace fromSignificantPart(String significantPart) {
            final String value = getPrefix() + significantPart;
            return new Namespace(value, this);
        }

        abstract char getPrefix();
    }
}
