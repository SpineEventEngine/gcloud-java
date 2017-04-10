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

package org.spine3.server.storage.datastore.dsnative;

import com.google.common.base.Objects;
import org.spine3.users.TenantId;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    public static Namespace of(String datastoreNamespace) {
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
                result = id.getDomain()
                           .getValue();
                break;
            case EMAIL:
                result = id.getEmail()
                           .getValue();
                break;
            case VALUE:
                result = id.getValue();
                break;
            case KIND_NOT_SET:
            default:
                throw new IllegalStateException(format("Tenant ID is not set. Kind of TenantId is %s.",
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
        return value;
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
}
