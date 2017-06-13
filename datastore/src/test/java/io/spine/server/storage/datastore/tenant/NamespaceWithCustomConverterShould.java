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

package io.spine.server.storage.datastore.tenant;

import com.google.cloud.datastore.Key;
import com.google.common.base.Converter;
import com.google.common.base.Optional;
import org.junit.BeforeClass;
import org.junit.Test;
import io.spine.string.Stringifiers;
import io.spine.server.storage.datastore.ProjectId;
import io.spine.users.TenantId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static io.spine.server.storage.datastore.tenant.TenantConverterRegistry.getNamespaceConverter;
import static io.spine.server.storage.datastore.tenant.TenantConverterRegistry.registerNamespaceConverter;

/**
 * @author Dmytro Dashenkov
 */
public class NamespaceWithCustomConverterShould {

    private static final ProjectId PROJECT_ID = ProjectId.of("arbitraryproject");

    @BeforeClass
    public static void setUp() {
        registerNamespaceConverter(PROJECT_ID, new CustomNamespaceConverter());
    }

    @Test
    public void construct_from_TenantId() {
        final String ns = "my.test.namespace.from.tenant.id";
        final TenantId tenantId = TenantId.newBuilder()
                                          .setValue(ns)
                                          .build();
        final Namespace namespace = Namespace.of(tenantId, PROJECT_ID);
        final Optional<? extends Converter<String, TenantId>> converter =
                getNamespaceConverter(PROJECT_ID);
        assertTrue(converter.isPresent());
        assertEquals(converter.get()
                              .reverse()
                              .convert(tenantId), namespace.getValue());
    }

    @Test
    public void construct_from_Key() {
        final String ns = "my.test.namespace.from.key";
        final Key key = Key.newBuilder(PROJECT_ID.getValue(), "some.kind", ns)
                           .build();
        final Namespace namespace = Namespace.fromNameOf(key, true);
        assertNotNull(namespace);
        assertEquals(ns, namespace.getValue());
    }

    @Test
    public void restore_to_TenantId() {
        final String ns = "my.test.namespace.to.tenant.id";
        final TenantId tenantId = TenantId.newBuilder()
                                          .setValue(ns)
                                          .build();
        final Namespace namespace = Namespace.of(tenantId, PROJECT_ID);
        assertEquals(tenantId, namespace.toTenantId());
    }

    /**
     * An example of custom {@link NamespaceToTenantIdConverter}.
     *
     * <p>Note that this implementation uses the default
     * {@link io.spine.string.Stringifier Stringifier} for the conversion, which is not acceptable
     * to use in production code, but good enough for these tests.
     */
    private static class CustomNamespaceConverter extends NamespaceToTenantIdConverter {

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
