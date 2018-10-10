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

import com.google.cloud.datastore.Key;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.spine.core.TenantId;
import io.spine.net.EmailAddress;
import io.spine.net.InternetDomain;
import io.spine.server.storage.datastore.ProjectId;
import io.spine.server.storage.datastore.TestDatastores;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.datastore.tenant.Namespace.of;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NamespaceTest {

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void testNulls() {
        new NullPointerTester()
                .setDefault(ProjectId.class, TestDatastores.projectId())
                .setDefault(TenantId.class, TenantId.getDefaultInstance())
                .setDefault(Key.class, Key.newBuilder(TestDatastores.projectId()
                                                                    .getValue(),
                                                      "kind",
                                                      "name")
                                          .build())
                .testStaticMethods(Namespace.class, NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    @DisplayName("not accept empty TenantIds")
    void testEmptyTenantId() {
        TenantId emptyId = TenantId.getDefaultInstance();
        assertThrows(IllegalArgumentException.class,
                     () -> of(emptyId, ProjectId.of("no-matter-what")));
    }

    @SuppressWarnings("LocalVariableNamingConvention")
        // Required comprehensive naming
    @Test
    @DisplayName("support equality")
    void testEquals() {
        String aGroupValue = "namespace1";
        TenantId aGroupTenantId = TenantId.newBuilder()
                                          .setValue(aGroupValue)
                                          .build();
        Namespace aGroupNamespaceFromTenantId = of(aGroupTenantId, TestDatastores.projectId());
        Namespace aGroupNamespaceFromString = of(aGroupValue);
        Namespace duplicateAGroupNamespaceFromString = of(aGroupValue);

        String bGroupValue = "namespace2";
        TenantId bGroupTenantId = TenantId.newBuilder()
                                          .setEmail(EmailAddress.newBuilder()
                                                                .setValue(bGroupValue))
                                          .build();
        Namespace bGroupNamespaceFromTenantId = of(bGroupTenantId, TestDatastores.projectId());
        // Same string but other type
        Namespace cGroupNamespaceFromString = of(bGroupValue);

        new EqualsTester()
                .addEqualityGroup(aGroupNamespaceFromTenantId)
                .addEqualityGroup(aGroupNamespaceFromString,
                                  duplicateAGroupNamespaceFromString)
                .addEqualityGroup(bGroupNamespaceFromTenantId)
                .addEqualityGroup(cGroupNamespaceFromString)
                .testEquals();
    }

    @Test
    @DisplayName("restore self to TenantId")
    void testToTenantId() {
        String randomTenantIdString = "arbitrary-tenant-id";
        TenantId domainId = TenantId.newBuilder()
                                    .setDomain(InternetDomain.newBuilder()
                                                             .setValue(randomTenantIdString))
                                    .build();
        TenantId emailId = TenantId.newBuilder()
                                   .setEmail(EmailAddress.newBuilder()
                                                         .setValue(randomTenantIdString))
                                   .build();
        TenantId stringId = TenantId.newBuilder()
                                    .setValue(randomTenantIdString)
                                    .build();
        assertNotEquals(domainId, emailId);
        assertNotEquals(domainId, stringId);
        assertNotEquals(emailId, stringId);

        Namespace fromDomainId = of(domainId, TestDatastores.projectId());
        Namespace fromEmailId = of(emailId, TestDatastores.projectId());
        Namespace fromStringId = of(stringId, TestDatastores.projectId());

        assertNotEquals(fromDomainId, fromEmailId);
        assertNotEquals(fromDomainId, fromStringId);
        assertNotEquals(fromEmailId, fromStringId);

        TenantId domainIdRestored = fromDomainId.toTenantId();
        TenantId emailIdRestored = fromEmailId.toTenantId();
        TenantId stringIdRestored = fromStringId.toTenantId();

        assertEquals(domainId, domainIdRestored);
        assertEquals(emailId, emailIdRestored);
        assertEquals(stringId, stringIdRestored);
    }

    @Test
    @DisplayName("return null if Key is empty")
    void testEmptyKey() {
        ProjectId projectId = ProjectId.of("project");
        Key emptyKey = Key.newBuilder(projectId.getValue(), "my.type", 42)
                          .build();
        Namespace namespace = Namespace.fromNameOf(emptyKey, false);
        assertNull(namespace);
    }

    @Test
    @DisplayName("construct from Key in single tenant mode")
    void testFromKeySingleTenant() {
        checkConstructFromKey("my.test.single.tenant.namespace.from.key", false);
    }

    @Test
    @DisplayName("construct from Key in multi-tenant mode")
    void testFromKeySingleMultitenant() {
        checkConstructFromKey("Vmy.test.single.tenant.namespace.from.key", true);
    }

    @Test
    @DisplayName("convert self to value-based TenantId if created from string")
    void testConvertToTenantId() {
        String namespaceString = "my.namespace";

        TenantId expectedId = TenantId.newBuilder()
                                      .setValue(namespaceString)
                                      .build();
        Namespace namespace = of(namespaceString);
        TenantId actualId = namespace.toTenantId();
        assertEquals(expectedId, actualId);
    }

    private static void checkConstructFromKey(String ns, boolean multitenant) {
        Key key = Key.newBuilder("my-simple-project", "any.kind", ns)
                     .build();
        Namespace namespace = Namespace.fromNameOf(key, multitenant);
        assertNotNull(namespace);
        assertEquals(ns, namespace.getValue());
    }
}
