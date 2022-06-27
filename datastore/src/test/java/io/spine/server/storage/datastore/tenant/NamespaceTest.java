/*
 * Copyright 2022, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.testing.server.storage.datastore.TestDatastores.defaultLocalProjectId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`Namespace` should")
class NamespaceTest {

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void testNulls() {
        ProjectId defaultProjectId = defaultLocalProjectId();
        Key defaultKey = Key.newBuilder(defaultProjectId.getValue(), "kind", "name")
                            .build();
        new NullPointerTester()
                .setDefault(ProjectId.class, defaultProjectId)
                .setDefault(TenantId.class, TenantId.getDefaultInstance())
                .setDefault(Key.class, defaultKey)
                .testStaticMethods(Namespace.class, NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    @DisplayName("not accept empty `TenantId`s")
    void testEmptyTenantId() {
        TenantId emptyId = TenantId.getDefaultInstance();
        assertThrows(IllegalArgumentException.class,
                     () -> Namespace.of(emptyId, true));
    }

    @SuppressWarnings("LocalVariableNamingConvention")
    // Required comprehensive naming
    @Test
    @DisplayName("support equality")
    void testEquals() {
        String aGroupValue = "namespace1";
        TenantId aGroupTenantId = TenantId
                .newBuilder()
                .setValue(aGroupValue)
                .vBuild();
        Namespace aGroupNamespaceFromTenantId =
                Namespace.of(aGroupTenantId, true, NsConverterFactory.defaults());
        Namespace aGroupNamespaceFromString = Namespace.of(aGroupValue);
        Namespace duplicateAGroupNamespaceFromString = Namespace.of(aGroupValue);

        String bGroupValue = "namespace2";
        EmailAddress bgGroupEmail = EmailAddress
                .newBuilder()
                .setValue(bGroupValue)
                .vBuild();
        TenantId bGroupTenantId = TenantId
                .newBuilder()
                .setEmail(bgGroupEmail)
                .vBuild();
        Namespace bGroupNamespaceFromTenantId = Namespace.of(bGroupTenantId,true);
        // Same string but other type
        Namespace cGroupNamespaceFromString = Namespace.of(bGroupValue);

        new EqualsTester()
                .addEqualityGroup(aGroupNamespaceFromTenantId)
                .addEqualityGroup(aGroupNamespaceFromString, duplicateAGroupNamespaceFromString)
                .addEqualityGroup(bGroupNamespaceFromTenantId)
                .addEqualityGroup(cGroupNamespaceFromString)
                .testEquals();
    }

    @Test
    @DisplayName("restore self to `TenantId`")
    void testToTenantId() {
        String randomTenantIdString = "arbitrary-tenant-id";
        InternetDomain internetDomain = InternetDomain
                .newBuilder()
                .setValue(randomTenantIdString)
                .vBuild();
        TenantId domainId = TenantId
                .newBuilder()
                .setDomain(internetDomain)
                .vBuild();
        EmailAddress emailAddress = EmailAddress
                .newBuilder()
                .setValue(randomTenantIdString)
                .vBuild();
        TenantId emailId = TenantId
                .newBuilder()
                .setEmail(emailAddress)
                .vBuild();
        TenantId stringId = TenantId
                .newBuilder()
                .setValue(randomTenantIdString)
                .vBuild();
        assertNotEquals(domainId, emailId);
        assertNotEquals(domainId, stringId);
        assertNotEquals(emailId, stringId);

        Namespace fromDomainId = Namespace.of(domainId, true);
        Namespace fromEmailId = Namespace.of(emailId, true);
        Namespace fromStringId = Namespace.of(stringId, true);

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
    @DisplayName("return null if `Key` is empty")
    void testEmptyKey() {
        ProjectId projectId = ProjectId.of("project");
        Key emptyKey = Key.newBuilder(projectId.getValue(), "my.type", 42)
                          .build();
        Namespace namespace = Namespace.fromNameOf(emptyKey, false);
        assertNull(namespace);
    }

    @Test
    @DisplayName("construct from `Key` in a single-tenant mode")
    void testFromKeySingleTenant() {
        checkConstructFromKey("my.test.single.tenant.namespace.from.key", false);
    }

    @Test
    @DisplayName("construct from `Key` in a multi-tenant mode")
    void testFromKeySingleMultitenant() {
        checkConstructFromKey("Vmy.test.single.tenant.namespace.from.key", true);
    }

    @Test
    @DisplayName("convert self to value-based `TenantId` if created from `String`")
    void testConvertToTenantId() {
        String namespaceString = "my.namespace";

        TenantId expectedId = TenantId
                .newBuilder()
                .setValue(namespaceString)
                .vBuild();
        Namespace namespace = Namespace.of(namespaceString);
        TenantId actualId = namespace.toTenantId();
        assertEquals(expectedId, actualId);
    }

    private static void checkConstructFromKey(String ns, boolean multitenant) {
        Key key = Key.newBuilder("my-simple-project", "any.kind", ns)
                     .build();
        Namespace namespace = Namespace.fromNameOf(key, multitenant);
        assertNotNull(namespace);
        assertEquals(ns, namespace.value());
    }
}
