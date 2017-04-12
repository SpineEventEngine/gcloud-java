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

import com.google.common.testing.EqualsTester;
import org.junit.Test;
import org.spine3.net.EmailAddress;
import org.spine3.users.TenantId;

/**
 * @author Dmytro Dashenkov
 */
public class NamespaceShould {

    @Test(expected = IllegalArgumentException.class)
    public void not_accept_empty_TenantIds() {
        final TenantId emptyId = TenantId.getDefaultInstance();
        Namespace.of(emptyId);
    }

    @SuppressWarnings("LocalVariableNamingConvention")
        // Required comprehensive naming
    @Test
    public void support_equality() {
        final String aGroupValue = "namespace1";
        final TenantId aGroupTenantId = TenantId.newBuilder()
                                                .setValue(aGroupValue)
                                                .build();
        final Namespace aGroupNamespaceFromTenantId = Namespace.of(aGroupTenantId);
        final Namespace aGroupnamespaceFromString = Namespace.of(aGroupValue);
        final Namespace duplicateAGroupnamespaceFromString = Namespace.of(aGroupValue);

        final String bGroupValue = "namespace2";
        final TenantId bGroupTenantId = TenantId.newBuilder()
                                                .setEmail(EmailAddress.newBuilder()
                                                                      .setValue(bGroupValue))
                                                .build();
        final Namespace bGroupNamespaceFromTenantId = Namespace.of(bGroupTenantId);
        final Namespace bGroupNamespaceFromString = Namespace.of(bGroupValue);

        new EqualsTester()
                .addEqualityGroup(aGroupNamespaceFromTenantId,
                                  aGroupnamespaceFromString,
                                  duplicateAGroupnamespaceFromString)
                .addEqualityGroup(bGroupNamespaceFromTenantId,
                                  bGroupNamespaceFromString)
                .testEquals();
    }
}
