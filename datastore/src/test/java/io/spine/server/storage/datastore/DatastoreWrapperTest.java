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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.protobuf.Any;
import io.spine.core.TenantId;
import io.spine.net.EmailAddress;
import io.spine.net.InternetDomain;
import io.spine.server.tenant.TenantAwareFunction0;
import io.spine.server.tenant.TenantAwareOperation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.collect.Lists.newArrayList;
import static io.spine.server.storage.datastore.DatastoreWrapper.wrap;
import static io.spine.server.storage.datastore.Entities.messageToEntity;
import static io.spine.server.storage.datastore.TestDatastoreWrapper.wrap;
import static io.spine.server.storage.datastore.given.Given.testProjectId;
import static io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers.multitenant;
import static io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers.singleTenant;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isIn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@DisplayName("DatastoreWrapper should")
class DatastoreWrapperTest {

    private static final String NAMESPACE_HOLDER_KIND = "spine.test.NAMESPACE_HOLDER_KIND";

    @AfterAll
    static void tearDown() {
        DatastoreWrapper wrapper = wrap(Given.testDatastore(), singleTenant());
        wrapper.dropTable(NAMESPACE_HOLDER_KIND);
    }

    @Nested
    class SingleTenant {

        private DatastoreWrapper wrapper;

        @BeforeEach
        void setUp() {
            wrapper = wrap(Given.testDatastore(), singleTenant());
            wrapper.startTransaction();
        }

        @Test
        @DisplayName("work with transactions if necessary")
        void testExecuteTransactions() {
            assertTrue(wrapper.isTransactionActive());
            wrapper.commitTransaction();
            assertFalse(wrapper.isTransactionActive());
        }

        @Test
        @DisplayName("rollback transactions")
        void testRollback() {
            assertTrue(wrapper.isTransactionActive());
            wrapper.rollbackTransaction();
            assertFalse(wrapper.isTransactionActive());
        }

        @Test
        @DisplayName("fail to start transaction if one is active")
        void testFailToRestartTransactions() {
            try {
                assertTrue(wrapper.isTransactionActive());
                assertThrows(IllegalStateException.class, wrapper::startTransaction);
            } finally {
                wrapper.rollbackTransaction();
            }
        }

        @Test
        @DisplayName("fail to finish non active transaction")
        void testFailToFinishNonActiveTransaction() {
            assertTrue(wrapper.isTransactionActive());
            wrapper.commitTransaction();
            assertFalse(wrapper.isTransactionActive());
            assertThrows(IllegalStateException.class, wrapper::rollbackTransaction);
        }
    }

    @Nested
    class NotWaiting {

        private TestDatastoreWrapper wrapper;

        @BeforeEach
        void setUp() {
            wrapper = wrap(Given.testDatastore(), false);
        }

        @AfterEach
        void tearDown() {
            wrapper.dropAllTables();
        }

        @Test
        @DisplayName("support bulk reads")
        void testBulkRead() throws InterruptedException {
            int bulkSize = 1001;

            Map<Key, Entity> entities = Given.nEntities(bulkSize, wrapper);
            Collection<Entity> expectedEntities = entities.values();

            wrapper.createOrUpdate(expectedEntities);

            // Wait for some time to make sure the writing is complete
            Thread.sleep(bulkSize * 5L);

            Collection<Entity> readEntities = newArrayList(wrapper.read(entities.keySet()));
            assertEquals(entities.size(), readEntities.size());
            assertTrue(expectedEntities.containsAll(readEntities));
        }

        @Disabled("This test rarely passes on Travis CI due to eventual consistency.")
        @Test
        @DisplayName("support big bulk reads")
        void testBigBulkRead() throws InterruptedException {
            int bulkSize = 2001;

            Map<Key, Entity> entities = Given.nEntities(bulkSize, wrapper);
            Collection<Entity> expectedEntities = entities.values();

            wrapper.createOrUpdate(expectedEntities);

            // Wait for some time to make sure the writing is complete
            Thread.sleep(bulkSize * 3L);

            StructuredQuery<Entity> query = Query.newEntityQueryBuilder()
                                                 .setKind(Given.GENERIC_ENTITY_KIND.getValue())
                                                 .build();
            Collection<Entity> readEntities = newArrayList(wrapper.read(query));
            assertEquals(entities.size(), readEntities.size());
            assertTrue(expectedEntities.containsAll(readEntities));
        }
    }

    @Test
    @DisplayName("generate key factories aware of tenancy")
    void testGenerateKeyFactory() {
        ProjectId projectId = ProjectId.of(TestDatastoreStorageFactory.DEFAULT_DATASET_NAME);
        DatastoreWrapper wrapper = wrap(Given.testDatastore(), multitenant(projectId));
        String tenantId1 = "first-tenant-ID";
        String tenantId1Prefixed = "Vfirst-tenant-ID";
        String tenantId2 = "second@tenant.id";
        String tenantId2Prefixed = "Esecond-at-tenant.id";
        String tenantId3 = "third.id";
        String tenantId3Prefixed = "Dthird.id";
        Datastore datastore = wrapper.getDatastore();
        ensureNamespace(tenantId1Prefixed, datastore);
        ensureNamespace(tenantId2Prefixed, datastore);
        ensureNamespace(tenantId3Prefixed, datastore);
        TenantId id1 = TenantId.newBuilder()
                               .setValue(tenantId1)
                               .build();
        TenantId id2 = TenantId.newBuilder()
                               .setEmail(EmailAddress.newBuilder()
                                                     .setValue(tenantId2))
                               .build();
        TenantId id3 = TenantId.newBuilder()
                               .setDomain(InternetDomain.newBuilder()
                                                        .setValue(tenantId3))
                               .build();

        checkTenantIdInKey(tenantId1Prefixed, id1, wrapper);
        checkTenantIdInKey(tenantId2Prefixed, id2, wrapper);
        checkTenantIdInKey(tenantId3Prefixed, id3, wrapper);
    }

    @Test
    @DisplayName("produce lazy iterator on query read")
    void testLazyIterator() {
        DatastoreWrapper wrapper = wrap(Given.testDatastore(), singleTenant());
        int count = 2;
        Map<?, Entity> entities = Given.nEntities(count, wrapper);
        Collection<Entity> expctedEntities = entities.values();
        wrapper.createOrUpdate(expctedEntities);

        StructuredQuery<Entity> query = Query.newEntityQueryBuilder()
                                             .setKind(Given.GENERIC_ENTITY_KIND.getValue())
                                             .build();
        Iterator<Entity> result = wrapper.read(query);

        assertTrue(result.hasNext());
        Entity first = result.next();
        assertTrue(result.hasNext());
        Entity second = result.next();

        assertThat(first, isIn(expctedEntities));
        assertThat(second, isIn(expctedEntities));

        assertFalse(result.hasNext());
        assertFalse(result.hasNext());
        assertFalse(result.hasNext());

        try {
            result.next();
            fail();
        } catch (NoSuchElementException ignored) {
        }
    }

    @Test
    @DisplayName("allow to add new namespaces 'on the go'")
    void testNewNamespaces() {
        DatastoreWrapper wrapper = wrap(Given.testDatastore(), multitenant(testProjectId()));
        TenantId tenantId = TenantId.newBuilder()
                                    .setValue("Luke_I_am_your_tenant.")
                                    .build();
        String key = "noooooo";
        Key entityKey = new TenantAwareFunction0<Key>(tenantId) {
            @Override
            public Key apply() {
                Key entityKey = wrapper.getKeyFactory(Kind.of(NAMESPACE_HOLDER_KIND))
                                       .newKey(key);
                Entity entity = Entity.newBuilder(entityKey)
                                      .build();
                wrapper.create(entity);
                return entityKey;
            }
        }.execute();

        // Clean up the namespace.
        wrapper.delete(entityKey);
    }

    private static void checkTenantIdInKey(String id, TenantId tenantId, DatastoreWrapper wrapper) {
        new TenantAwareOperation(tenantId) {
            @Override
            public void run() {
                Key key = wrapper.getKeyFactory(Given.GENERIC_ENTITY_KIND)
                                 .newKey(42L);
                assertEquals(id, key.getNamespace());
            }
        }.execute();
    }

    private static void ensureNamespace(String namespaceValue, Datastore datastore) {
        KeyFactory keyFactory = datastore.newKeyFactory()
                                         .setNamespace(namespaceValue)
                                         .setKind(NAMESPACE_HOLDER_KIND);
        Entity entity = Entity.newBuilder(keyFactory.newKey(42L))
                              .build();
        datastore.put(entity);
    }

    private static class Given {

        private static final Kind GENERIC_ENTITY_KIND = Kind.of("my.entity");

        private static Datastore testDatastore() {
            boolean onCi = TestEnvironment.runsOnCi();
            return onCi
                   ? TestDatastores.remote()
                   : TestDatastores.local();
        }

        private static Map<Key, Entity> nEntities(int n, DatastoreWrapper wrapper) {
            Map<Key, Entity> result = new HashMap<>(n);
            for (int i = 0; i < n; i++) {
                Any message = Any.getDefaultInstance();
                RecordId recordId = new RecordId(String.format("record-%s", i));
                Key key = wrapper.keyFor(GENERIC_ENTITY_KIND, recordId);
                Entity entity = messageToEntity(message, key);
                result.put(key, entity);
            }
            return result;
        }
    }
}
