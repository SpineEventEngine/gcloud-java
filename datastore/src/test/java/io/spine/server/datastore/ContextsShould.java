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

package io.spine.server.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.testing.NullPointerTester;
import io.spine.server.BoundedContext;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.DatastoreStorageFactory;
import io.spine.server.storage.datastore.TestDatastoreFactory;
import io.spine.server.storage.datastore.tenant.TestNamespaceIndex;
import io.spine.server.tenant.TenantIndex;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory.defaultInstance;
import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Dmytro Dashenkov
 */
public class ContextsShould {

    @Test
    public void have_private_util_ctor() {
        assertHasPrivateParameterlessCtor(Contexts.class);
    }

    @Test
    public void not_accept_nulls() {
        new NullPointerTester()
                .testAllPublicStaticMethods(Contexts.class);
    }

    @Test
    public void produce_BoundedContext_Builder_for_given_storage_factory() {
        DatastoreStorageFactory factory = givenFactory();
        BoundedContext.Builder builder = Contexts.onTopOf(factory);
        Optional<Supplier<StorageFactory>> supplierOptional =
                builder.getStorageFactorySupplier();
        assertTrue(supplierOptional.isPresent());
        assertSame(factory, supplierOptional.get().get());
        assertEquals(builder.isMultitenant(), factory.isMultitenant());
        Optional<? extends TenantIndex> tenantIndexOptional = builder.getTenantIndex();
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
        return TestDatastoreFactory.getLocalDatastore();
    }
}
