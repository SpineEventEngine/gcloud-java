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

import org.spine3.server.tenant.TenantFunction;
import org.spine3.users.TenantId;

import javax.annotation.Nullable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * A {@link NamespaceSupplier} for multitenant storage factories.
 *
 * @author Dmytro Dashenkov
 */
final class MultitenantNamespaceSupplier extends NamespaceSupplier {

    private static final Pattern AT_SYMBOL_PATTERN = Pattern.compile("@", Pattern.LITERAL);
    private static final String AT_SYMBOL_REPLACEMENT = "-at-";

    /**
     * {@inheritDoc}
     *
     * @return the {@link Namespace} representing the current tenant {@link TenantId ID}
     */
    @Override
    public Namespace getNamespace() {
        final TenantIdRetriever retriever = new TenantIdRetriever();
        final TenantId tenantId = retriever.execute();
        checkNotNull(tenantId);
        final String stringTenantId = tenantIdToSignificantString(tenantId);
        final Namespace result = Namespace.of(stringTenantId);
        return result;
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

        final String escapedResult = escapeIllegalCharacters(result);
        return escapedResult;
    }

    private static String escapeIllegalCharacters(String condidateNamespace) {
        final String result = AT_SYMBOL_PATTERN.matcher(condidateNamespace)
                                               .replaceAll(Matcher.quoteReplacement(AT_SYMBOL_REPLACEMENT));
        return result;
    }

    private static class TenantIdRetriever extends TenantFunction<TenantId> {

        private TenantIdRetriever() {
            super(true);
        }

        @Override
        public TenantId apply(@Nullable TenantId input) {
            checkNotNull(input);
            return input;
        }
    }
}
