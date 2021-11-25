/*
 * Copyright 2021, TeamDev. All rights reserved.
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
import io.spine.core.TenantId;
import io.spine.server.storage.datastore.ProjectId;
import io.spine.string.Stringifier;
import io.spine.string.Stringifiers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("`Namespace` with custom converter should")
final class NamespaceWithCustomConverterTest {

    private static final ProjectId PROJECT_ID = ProjectId.of("arbitraryproject");
    @SuppressWarnings("UnnecessaryLambda")
    private static final NsConverterFactory factory = multitenant -> new CustomNamespaceConverter();

    @Test
    @DisplayName("construct from `TenantId`")
    void testFromTenantId() {
        var ns = "my.test.namespace.from.tenant.id";
        var tenantId = TenantId.newBuilder()
                .setValue(ns)
                .vBuild();
        var multitenant = true;
        var namespace = Namespace.of(tenantId, multitenant, factory);
        var converter = factory.get(multitenant);
        assertNotNull(converter);
        assertEquals(converter.reverse()
                              .convert(tenantId), namespace.value());
    }

    @Test
    @DisplayName("construct from `Key`")
    void testFromKey() {
        var ns = "my.test.namespace.from.key";
        var key = Key
                .newBuilder(PROJECT_ID.value(), "some.kind", ns)
                .build();
        var namespace = Namespace.fromNameOf(key, true, factory);
        assertNotNull(namespace);
        assertEquals(ns, namespace.value());
    }

    @Test
    @DisplayName("restore to `TenantId`")
    void testToTenantId() {
        var ns = "my.test.namespace.to.tenant.id";
        var tenantId = TenantId.newBuilder()
                .setValue(ns)
                .vBuild();
        var namespace = Namespace.of(tenantId, true, factory);
        assertEquals(tenantId, namespace.toTenantId());
    }

    /**
     * An example of custom {@link NamespaceConverter}.
     *
     * <p>Note that this implementation uses the default
     * {@link Stringifier Stringifier} for the conversion, which is not acceptable
     * to use in production code, but good enough for these tests.
     */
    private static class CustomNamespaceConverter extends NamespaceConverter {

        @Override
        protected String toString(TenantId tenantId) {
            return Stringifiers.toString(tenantId);
        }

        @Override
        protected TenantId toTenantId(String namespace) {
            return Stringifiers.fromString(namespace, TenantId.class);
        }
    }
}
