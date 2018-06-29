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

import org.junit.jupiter.api.Test;
import io.spine.core.TenantId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;

/**
 * @author Dmytro Dashenkov
 */
public class NamespaceConvertersShould {

    @Test
    public void have_private_utility_ctor() {
        assertHasPrivateParameterlessCtor(NamespaceConverters.class);
    }

    @Test
    public void create_stub_faulty_converter() {
        final NamespaceToTenantIdConverter converter = NamespaceConverters.stub();
        try {
            converter.convert("");
            fail();
        } catch (UnsupportedOperationException ignored) {
        }

        try {
            converter.reverse()
                     .convert(TenantId.getDefaultInstance());
            fail();
        } catch (UnsupportedOperationException ignored) {
        }
    }

    @Test
    public void create_custom_namespace_converter() {
        final NamespaceToTenantIdConverter converter = NamespaceConverters.forCustomNamespace();
        final Namespace namespace = Namespace.of("namespace");
        final TenantId fromInternalConverter = namespace.toTenantId();
        final TenantId fromExternalConverter = converter.convert(namespace.getValue());

        assertEquals(fromInternalConverter, fromExternalConverter);

        final String restored = converter.reverse().convert(fromExternalConverter);
        assertEquals(namespace.getValue(), restored);
    }
}
