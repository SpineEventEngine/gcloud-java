/*
 * Copyright 2020, TeamDev. All rights reserved.
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
import io.spine.server.tenant.TenantIndex;
import io.spine.testing.server.storage.datastore.TestDatastores;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("`DatastoreTenants` should")
class DatastoreTenantsTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void havePrivateUtilityCtor() {
        assertHasPrivateParameterlessCtor(DatastoreTenants.class);
    }

    @Test
    @DisplayName("create tenant index")
    void testCreateIndex() {
        TenantIndex index =
                DatastoreTenants.index(TestDatastores.local(), NsConverterFactory.defaults());
        assertNotNull(index);
        assertThat(index).isInstanceOf(NamespaceIndex.class);

        String customNamespace = "Vmy-namespace";
        TenantId customId = TenantId
                .newBuilder()
                .setValue(customNamespace)
                .vBuild();
        index.keep(customId);

        Set<TenantId> ids = index.all();
        assertThat(ids).contains(customId);
    }
}
