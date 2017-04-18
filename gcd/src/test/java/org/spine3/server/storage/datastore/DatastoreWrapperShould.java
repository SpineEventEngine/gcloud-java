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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.protobuf.Any;
import org.junit.AfterClass;
import org.junit.Test;
import org.spine3.net.EmailAddress;
import org.spine3.net.InternetDomain;
import org.spine3.server.storage.datastore.tenant.TestNamespaceSuppliers;
import org.spine3.server.tenant.TenantAwareOperation;
import org.spine3.users.TenantId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class DatastoreWrapperShould {

    private static final String NAMESPACE_HOLDER_KIND = "spine.test.NAMESPACE_HOLDER_KIND";

    @AfterClass
    public static void tearDown() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               TestNamespaceSuppliers.singleTenant());
        wrapper.dropTable(NAMESPACE_HOLDER_KIND);
    }

    @Test
    public void work_with_transactions_if_necessary() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               TestNamespaceSuppliers.singleTenant());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.commitTransaction();
        assertFalse(wrapper.isTransactionActive());
    }

    @Test
    public void rollback_transactions() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               TestNamespaceSuppliers.singleTenant());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.rollbackTransaction();
        assertFalse(wrapper.isTransactionActive());
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_start_transaction_if_one_is_active() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               TestNamespaceSuppliers.singleTenant());
        try {
            wrapper.startTransaction();
            assertTrue(wrapper.isTransactionActive());
            wrapper.startTransaction();
        } finally {
            wrapper.rollbackTransaction();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_finish_not_active_transaction() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               TestNamespaceSuppliers.singleTenant());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.commitTransaction();
        assertFalse(wrapper.isTransactionActive());
        wrapper.rollbackTransaction();
    }

    // Some variables act in different cases and should have self-explainatory names
    @SuppressWarnings("UnnecessaryLocalVariable")
    @Test
    public void support_big_bulk_reads() throws InterruptedException {
        final int bulkSize = 1001;

        final TestDatastoreWrapper wrapper = TestDatastoreWrapper.wrap(Given.testDatastore(), false);
        final Map<Key, Entity> entities = Given.nEntities(bulkSize, wrapper);
        final Collection<Entity> expectedEntities = entities.values();

        wrapper.createOrUpdate(expectedEntities);

        // Wait for some time to make sure the writing is complete
        try {
            Thread.sleep(bulkSize * 5);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }

        final Collection<Entity> readEntities = wrapper.read(entities.keySet());
        assertEquals(entities.size(), readEntities.size());
        assertTrue(expectedEntities.containsAll(readEntities));

        wrapper.dropAllTables();
    }

    @Test
    public void generate_key_factories_aware_of_tenancy() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               TestNamespaceSuppliers.multitenant());
        final String tenantId1 = "first-tenant-ID";
        final String tenantId1Prefixed = "Vfirst-tenant-ID";
        final String tenantId2 = "second@tenant.id";
        final String tenantId2Prefixed = "Esecond-at-tenant.id";
        final String tenantId3 = "third.id";
        final String tenantId3Prefixed = "Dthird.id";
        ensureNamespace(tenantId1Prefixed, wrapper.getDatastore());
        ensureNamespace(tenantId2Prefixed, wrapper.getDatastore());
        ensureNamespace(tenantId3Prefixed, wrapper.getDatastore());
        final TenantId id1 = TenantId.newBuilder()
                                     .setValue(tenantId1)
                                     .build();
        final TenantId id2 = TenantId.newBuilder()
                                     .setEmail(EmailAddress.newBuilder()
                                                           .setValue(tenantId2))
                                     .build();
        final TenantId id3 = TenantId.newBuilder()
                                     .setDomain(InternetDomain.newBuilder()
                                                              .setValue(tenantId3))
                                     .build();

        checkTenantIdInKey(tenantId1Prefixed, id1, wrapper);
        checkTenantIdInKey(tenantId2Prefixed, id2, wrapper);
        checkTenantIdInKey(tenantId3Prefixed, id3, wrapper);
    }

    private static void checkTenantIdInKey(final String id, TenantId tenantId, final DatastoreWrapper wrapper) {
        new TenantAwareOperation(tenantId) {
            @Override
            public void run() {
                final Key key = wrapper.getKeyFactory(Given.GENERIC_ENTITY_KIND)
                                       .newKey(42L);
                assertEquals(id, key.getNamespace());
            }
        }.execute();
    }

    private static void ensureNamespace(String namespaceValue, Datastore datastore) {
        final KeyFactory keyFactory = datastore.newKeyFactory()
                                               .setNamespace(namespaceValue)
                                               .setKind(NAMESPACE_HOLDER_KIND);
        final Entity entity = Entity.newBuilder(keyFactory.newKey(42L))
                                    .build();
        datastore.put(entity);
    }

    private static class Given {

        private static final Kind GENERIC_ENTITY_KIND = Kind.of("my.entity");

        private static Datastore testDatastore() {
            final boolean onCi = "true".equals(System.getenv("CI"));
            return onCi
                   ? TestDatastoreStorageFactory.TestingDatastoreSingleton.INSTANCE.value
                   : TestDatastoreStorageFactory.DefaultDatastoreSingleton.INSTANCE.value;
        }

        private static Map<Key, Entity> nEntities(int n, DatastoreWrapper wrapper) {
            final Map<Key, Entity> result = new HashMap<>(n);
            for (int i = 0; i < n; i++) {
                final Any message = Any.getDefaultInstance();
                final DatastoreRecordId recordId = new DatastoreRecordId(String.format("record-%s", i));
                final Key key = DsIdentifiers.keyFor(wrapper, GENERIC_ENTITY_KIND, recordId);
                final Entity entity = Entities.messageToEntity(message, key);
                result.put(key, entity);
            }
            return result;
        }
    }
}
