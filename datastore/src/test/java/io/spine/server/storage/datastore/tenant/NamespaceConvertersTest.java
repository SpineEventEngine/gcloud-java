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

import io.spine.core.TenantId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.datastore.given.TestCases.HAVE_PRIVATE_UTILITY_CTOR;
import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("NamespaceConverters should")
class NamespaceConvertersTest {

    @Test
    @DisplayName(HAVE_PRIVATE_UTILITY_CTOR)
    void have_private_utility_ctor() {
        assertHasPrivateParameterlessCtor(NamespaceConverters.class);
    }

    @Test
    @DisplayName("create stub faulty converter")
    void testFaulty() {
        NamespaceToTenantIdConverter converter = NamespaceConverters.stub();
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
    @DisplayName("create custom namespace converter")
    void testCustomConverter() {
        NamespaceToTenantIdConverter converter = NamespaceConverters.forCustomNamespace();
        Namespace namespace = Namespace.of("namespace");
        TenantId fromInternalConverter = namespace.toTenantId();
        TenantId fromExternalConverter = converter.convert(namespace.getValue());

        assertEquals(fromInternalConverter, fromExternalConverter);

        String restored = converter.reverse().convert(fromExternalConverter);
        assertEquals(namespace.getValue(), restored);
    }
}