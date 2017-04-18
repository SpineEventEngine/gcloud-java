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
import org.junit.BeforeClass;
import org.junit.Test;
import org.spine3.base.Stringifiers;
import org.spine3.users.TenantId;

import static org.junit.Assert.assertEquals;

/**
 * @author Dmytro Dashenkov
 */
public class NamespaceWithCustomConverterShould {

    @BeforeClass
    public static void setUp() {
        DatastoreTenants.registerNamespaceConverter(new CustomNamespaceConverter());
    }

    @Test
    public void construct_from_TenantId() {
        final String ns = "my.test.namespace.from.tenant.id";
        final TenantId tenantId = TenantId.newBuilder()
                                          .setValue(ns)
                                          .build();
        final Namespace namespace = Namespace.of(tenantId);
        assertEquals(Stringifiers.toString(tenantId), namespace.getValue());
    }

    @Test
    public void construct_from_Key() {
        final String ns = "my.test.namespace.from.key";
        final Key key = Key.newBuilder("my-project", "some.kind", ns)
                           .build();
        final Namespace namespace = Namespace.fromNameOf(key);
        assertEquals(ns, namespace.getValue());
    }

    @Test
    public void restore_to_TenantId() {
        final String ns = "my.test.namespace.to.tenant.id";
        final TenantId tenantId = TenantId.newBuilder()
                                          .setValue(ns)
                                          .build();
        final Namespace namespace = Namespace.of(tenantId);
        assertEquals(tenantId, namespace.toTenantId());
    }

    /**
     * An example of custom {@link NamespaceToTenantIdConverter}.
     *
     * <p>Note that this implementation uses the default
     * {@link org.spine3.base.Stringifier Stringifier} for the convertion, which is not acceptable
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
