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
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import org.junit.Test;
import org.spine3.net.EmailAddress;
import org.spine3.net.InternetDomain;
import org.spine3.server.storage.datastore.ProjectId;
import org.spine3.users.TenantId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.spine3.server.storage.datastore.Given.TEST_PROJECT_ID;
import static org.spine3.server.storage.datastore.Given.TEST_PROJECT_ID_VALUE;

/**
 * @author Dmytro Dashenkov
 */
public class NamespaceShould {

    @Test
    public void not_accept_nulls() {
        new NullPointerTester()
                .setDefault(ProjectId.class, TEST_PROJECT_ID)
                .setDefault(TenantId.class, TenantId.getDefaultInstance())
                .setDefault(Key.class, Key.newBuilder(TEST_PROJECT_ID_VALUE,
                                                      "kind",
                                                      "name")
                                          .build())
                .testStaticMethods(Namespace.class, NullPointerTester.Visibility.PACKAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_accept_empty_TenantIds() {
        final TenantId emptyId = TenantId.getDefaultInstance();
        Namespace.of(emptyId, ProjectId.of("no-matter-what"));
    }

    @SuppressWarnings("LocalVariableNamingConvention")
        // Required comprehensive naming
    @Test
    public void support_equality() {
        final String aGroupValue = "namespace1";
        final TenantId aGroupTenantId = TenantId.newBuilder()
                                                .setValue(aGroupValue)
                                                .build();
        final Namespace aGroupNamespaceFromTenantId = Namespace.of(aGroupTenantId, TEST_PROJECT_ID);
        final Namespace aGroupNamespaceFromString = Namespace.of(aGroupValue);
        final Namespace duplicateAGroupNamespaceFromString = Namespace.of(aGroupValue);

        final String bGroupValue = "namespace2";
        final TenantId bGroupTenantId = TenantId.newBuilder()
                                                .setEmail(EmailAddress.newBuilder()
                                                                      .setValue(bGroupValue))
                                                .build();
        final Namespace bGroupNamespaceFromTenantId = Namespace.of(bGroupTenantId, TEST_PROJECT_ID);
        // Same string but other type
        final Namespace cGroupNamespaceFromString = Namespace.of(bGroupValue);

        new EqualsTester()
                .addEqualityGroup(aGroupNamespaceFromTenantId)
                .addEqualityGroup(aGroupNamespaceFromString,
                                  duplicateAGroupNamespaceFromString)
                .addEqualityGroup(bGroupNamespaceFromTenantId)
                .addEqualityGroup(cGroupNamespaceFromString)
                .testEquals();
    }

    @Test
    public void restore_self_to_tenant_id() {
        final String randomTenantIdString = "arbitrary-tenant-id";
        final TenantId domainId = TenantId.newBuilder()
                                          .setDomain(InternetDomain.newBuilder()
                                                                   .setValue(randomTenantIdString))
                                          .build();
        final TenantId emailId = TenantId.newBuilder()
                                         .setEmail(EmailAddress.newBuilder()
                                                               .setValue(randomTenantIdString))
                                         .build();
        final TenantId stringId = TenantId.newBuilder()
                                          .setValue(randomTenantIdString)
                                          .build();
        assertNotEquals(domainId, emailId);
        assertNotEquals(domainId, stringId);
        assertNotEquals(emailId, stringId);

        final Namespace fromDomainId = Namespace.of(domainId, TEST_PROJECT_ID);
        final Namespace fromEmailId = Namespace.of(emailId, TEST_PROJECT_ID);
        final Namespace fromStringId = Namespace.of(stringId, TEST_PROJECT_ID);

        assertNotEquals(fromDomainId, fromEmailId);
        assertNotEquals(fromDomainId, fromStringId);
        assertNotEquals(fromEmailId, fromStringId);

        final TenantId domainIdRestored = fromDomainId.toTenantId();
        final TenantId emailIdRestored = fromEmailId.toTenantId();
        final TenantId stringIdRestored = fromStringId.toTenantId();

        assertEquals(domainId, domainIdRestored);
        assertEquals(emailId, emailIdRestored);
        assertEquals(stringId, stringIdRestored);
    }

    @Test
    public void return_null_if_key_is_empty() {
        final ProjectId projectId = ProjectId.of("project");
        final Key emptyKey = Key.newBuilder(projectId.getValue(), "my.type", 42)
                                .build();
        final Namespace namespace = Namespace.fromNameOf(emptyKey, false);
        assertNull(namespace);
    }

    @Test
    public void construct_from_Key_in_single_tenant() {
        checkConstructFromKey("my.test.single.tenant.namespace.from.key", false);
    }

    @Test
    public void construct_from_Key_in_multitenant() {
        checkConstructFromKey("Vmy.test.single.tenant.namespace.from.key", true);
    }

    private static void checkConstructFromKey(String ns, boolean multitenant) {
        final Key key = Key.newBuilder("my-simple-project", "any.kind", ns)
                           .build();
        final Namespace namespace = Namespace.fromNameOf(key, multitenant);
        assertNotNull(namespace);
        assertEquals(ns, namespace.getValue());
    }

    @Test
    public void convert_self_to_string_based_tenant_if_created_from_string() {
        final String namespaceString = "my.namespace";

        final TenantId expectedId = TenantId.newBuilder()
                                            .setValue(namespaceString)
                                            .build();
        final Namespace namespace = Namespace.of(namespaceString);
        final TenantId actualId = namespace.toTenantId();
        assertEquals(expectedId, actualId);
    }
}
