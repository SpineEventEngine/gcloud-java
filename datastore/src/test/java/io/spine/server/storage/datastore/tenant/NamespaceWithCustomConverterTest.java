/*
 * Copyright 2019, TeamDev. All rights reserved.
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.spine.server.storage.datastore.tenant.TenantConverterRegistry.getNamespaceConverter;
import static io.spine.server.storage.datastore.tenant.TenantConverterRegistry.registerNamespaceConverter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Namespace with custom converter should")
class NamespaceWithCustomConverterTest {

    private static final ProjectId PROJECT_ID = ProjectId.of("arbitraryproject");

    @BeforeAll
    static void setUp() {
        registerNamespaceConverter(PROJECT_ID, new CustomNamespaceConverter());
    }

    @Test
    @DisplayName("construct from TenantId")
    void testFromTenantId() {
        String ns = "my.test.namespace.from.tenant.id";
        TenantId tenantId = TenantId
                .newBuilder()
                .setValue(ns)
                .vBuild();
        Namespace namespace = Namespace.of(tenantId, PROJECT_ID);
        Optional<NamespaceConverter> converter =
                getNamespaceConverter(PROJECT_ID);
        assertTrue(converter.isPresent());
        assertEquals(converter.get()
                              .reverse()
                              .convert(tenantId), namespace.getValue());
    }

    @Test
    @DisplayName("construct from Key")
    void testFromKey() {
        String ns = "my.test.namespace.from.key";
        Key key = Key.newBuilder(PROJECT_ID.getValue(), "some.kind", ns)
                     .build();
        Namespace namespace = Namespace.fromNameOf(key, true);
        assertNotNull(namespace);
        assertEquals(ns, namespace.getValue());
    }

    @Test
    @DisplayName("restore to TenantId")
    void testToTenantId() {
        String ns = "my.test.namespace.to.tenant.id";
        TenantId tenantId = TenantId
                .newBuilder()
                .setValue(ns)
                .vBuild();
        Namespace namespace = Namespace.of(tenantId, PROJECT_ID);
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
