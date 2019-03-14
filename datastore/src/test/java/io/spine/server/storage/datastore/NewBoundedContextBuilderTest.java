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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import io.spine.server.BoundedContextBuilder;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.given.TestDatastores;
import io.spine.server.storage.datastore.tenant.TestNamespaceIndex;
import io.spine.server.tenant.TenantIndex;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Supplier;

import static io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory.defaultInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("DatastoreStorageFactory should")
class NewBoundedContextBuilderTest {

    @Test
    @DisplayName("create BoundedContextBuilder")
    void testProduceBCBuilder() {
        DatastoreStorageFactory factory = givenFactory();
        BoundedContextBuilder builder = factory.newBoundedContextBuilder();
        Optional<Supplier<StorageFactory>> supplierOptional =
                builder.storageFactorySupplier();
        assertTrue(supplierOptional.isPresent());
        assertSame(factory, supplierOptional.get().get());
        assertEquals(builder.isMultitenant(), factory.isMultitenant());
        Optional<? extends TenantIndex> tenantIndexOptional = builder.tenantIndex();
        assertTrue(tenantIndexOptional.isPresent());
        assertThat(tenantIndexOptional.get(), instanceOf(TestNamespaceIndex.getType()));
    }

    private static DatastoreStorageFactory givenFactory() {
        DatastoreStorageFactory result =
                DatastoreStorageFactory.newBuilder()
                                       .setDatastore(givenDatastore())
                                       .setMultitenant(true)
                                       .setTypeRegistry(defaultInstance())
                                       .build();
        return result;
    }

    private static Datastore givenDatastore() {
        return TestDatastores.local();
    }
}
