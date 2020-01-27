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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import io.spine.core.TenantId;
import io.spine.net.EmailAddress;
import io.spine.net.InternetDomain;
import io.spine.server.tenant.TenantAwareFunction0;
import io.spine.testing.server.storage.datastore.TestDatastoreWrapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.cloud.datastore.Query.newEntityQueryBuilder;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.datastore.DatastoreWrapper.wrap;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.GENERIC_ENTITY_KIND;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.NAMESPACE_HOLDER_KIND;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.checkTenantIdInKey;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.ensureNamespace;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.localDatastore;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.remoteDatastore;
import static io.spine.server.storage.datastore.given.TestEnvironment.runsOnCi;
import static io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers.multitenant;
import static io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers.singleTenant;
import static io.spine.testing.server.storage.datastore.TestDatastoreWrapper.wrap;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisplayName("`DatastoreWrapper` should")
class DatastoreWrapperTest {

    @AfterAll
    static void tearDown() {
        DatastoreWrapper wrapper = wrap(localDatastore(), singleTenant());
        wrapper.dropTable(NAMESPACE_HOLDER_KIND);
    }

    @Nested
    class NotWaiting {

        private TestDatastoreWrapper wrapper;

        @BeforeEach
        void setUp() {
            wrapper = wrap(localDatastore(), false);
        }

        @AfterEach
        void tearDown() {
            wrapper.dropAllTables();
        }

        @Test
        @DisplayName("support bulk reads")
        void testBulkRead() throws InterruptedException {
            int bulkSize = 1001;

            Map<Key, Entity> entities = newTestEntities(bulkSize, wrapper);
            Collection<Entity> expectedEntities = entities.values();

            wrapper.createOrUpdate(expectedEntities);

            // Wait for some time to make sure the writing is complete
            Thread.sleep(bulkSize * 5L);

            Collection<Entity> readEntities = newArrayList(wrapper.read(entities.keySet()));
            assertEquals(entities.size(), readEntities.size());
            assertTrue(expectedEntities.containsAll(readEntities));
        }

        @Test
        @DisplayName("support big bulk reads")
        void testBigBulkRead() throws InterruptedException {
            int bulkSize = 2001;

            Map<Key, Entity> entities = newTestEntities(bulkSize, wrapper);
            Collection<Entity> expectedEntities = entities.values();

            wrapper.createOrUpdate(expectedEntities);

            // Wait for some time to make sure the writing is complete
            Thread.sleep(bulkSize * 3L);

            StructuredQuery<Entity> query = newEntityQueryBuilder()
                    .setKind(GENERIC_ENTITY_KIND.value())
                    .build();
            Collection<Entity> readEntities = newArrayList(wrapper.read(query));
            assertEquals(entities.size(), readEntities.size());
            assertTrue(expectedEntities.containsAll(readEntities));
        }
    }

    /**
     * The acceptance test for the remote datastore.
     *
     * <p>The suite performs basic read/write operations on the real datastore of the
     * {@code spine-dev} project. The purpose of the test is to make sure that everything related
     * to the real storage configuration (like indices) is done correctly.
     *
     * <p>Runs only on CI.
     */
    @Nested
    class Remote {

        private TestDatastoreWrapper wrapper;

        @BeforeEach
        void setUp() {
            wrapper = wrap(remoteDatastore(), true);
        }

        @AfterEach
        void tearDown() {
            wrapper.dropAllTables();
        }

        @Test
        @DisplayName("read and write entities in the remote datastore")
        void testBulkRead() {
            assumeTrue(runsOnCi());

            int entityCount = 5;
            Map<Key, Entity> entities = newTestEntities(entityCount, wrapper);
            Collection<Entity> expectedEntities = entities.values();

            wrapper.createOrUpdate(expectedEntities);

            Collection<Entity> readEntities = newArrayList(wrapper.read(entities.keySet()));
            assertEquals(entities.size(), readEntities.size());
            assertTrue(expectedEntities.containsAll(readEntities));
        }
    }

    @Nested
    @DisplayName("read entities by keys")
    class ReadByKeys {

        private TestDatastoreWrapper wrapper;

        @BeforeEach
        void setUp() {
            wrapper = wrap(localDatastore(), false);
        }

        @AfterEach
        void tearDown() {
            wrapper.dropAllTables();
        }

        @Test
        @DisplayName("replacing missing entities with null")
        void testMissingAreNull() throws InterruptedException {
            int bulkSize = 3;

            Map<Key, Entity> entities = createAndStoreTestEntities(bulkSize);

            // Wait for some time to make sure the writing is complete
            Thread.sleep(bulkSize * 5L);

            List<Key> presentKeys = newArrayList(entities.keySet());

            List<Key> queryKeys = new ImmutableList.Builder<Key>()
                    .add(newKey("missing-key-1", wrapper))
                    .add(presentKeys.get(0))
                    .add(presentKeys.get(1))
                    .add(newKey("missing-key-2", wrapper))
                    .add(presentKeys.get(2))
                    .build();

            Iterator<Entity> actualEntities = wrapper.read(queryKeys);

            assertNull(actualEntities.next());
            assertEquals(entities.get(presentKeys.get(0)), actualEntities.next());
            assertEquals(entities.get(presentKeys.get(1)), actualEntities.next());
            assertNull(actualEntities.next());
            assertEquals(entities.get(presentKeys.get(2)), actualEntities.next());

            assertFalse(actualEntities.hasNext());
        }

        @Test
        @DisplayName("preserving order")
        void test() throws InterruptedException {
            int bulkSize = 3;

            Map<Key, Entity> entities = createAndStoreTestEntities(bulkSize);

            // Wait for some time to make sure the writing is complete
            Thread.sleep(bulkSize * 5L);

            List<Key> presentKeys = newArrayList(entities.keySet());

            List<Key> queryKeys = new ImmutableList.Builder<Key>()
                    .add(presentKeys.get(2))
                    .add(presentKeys.get(0))
                    .add(presentKeys.get(1))
                    .build();

            Iterator<Entity> actualEntities = wrapper.read(queryKeys);

            assertEquals(entities.get(queryKeys.get(0)), actualEntities.next());
            assertEquals(entities.get(queryKeys.get(1)), actualEntities.next());
            assertEquals(entities.get(queryKeys.get(2)), actualEntities.next());

            assertFalse(actualEntities.hasNext());
        }

        private Map<Key, Entity> createAndStoreTestEntities(int bulkSize) {
            Map<Key, Entity> entities = newTestEntities(bulkSize, wrapper);
            Collection<Entity> expectedEntities = entities.values();
            wrapper.createOrUpdate(expectedEntities);
            return entities;
        }
    }

    @Nested
    @DisplayName("read with structured query")
    class ReadWithStructuredQuery {

        private TestDatastoreWrapper wrapper;

        @BeforeEach
        void setUp() {
            wrapper = wrap(localDatastore(), false);
        }

        @AfterEach
        void tearDown() {
            wrapper.dropAllTables();
        }

        @SuppressWarnings({"CheckReturnValue", "ResultOfMethodCallIgnored"})
        // Called to throw exception.
        @Test
        @DisplayName("throws IAE during batch read with limit")
        void testIaeOnBatchReadWithLimit() {
            assertThrows(IllegalArgumentException.class,
                         () -> wrapper.readAll(newEntityQueryBuilder()
                                                       .setLimit(5)
                                                       .build(), 1));
        }

        @SuppressWarnings({"CheckReturnValue", "ResultOfMethodCallIgnored"})
        // Called to throw exception.
        @Test
        @DisplayName("throws IAE during batch read of 0 size")
        void testIaeOnBatchReadWithZeroSize() {
            assertThrows(IllegalArgumentException.class,
                         () -> wrapper.readAll(newEntityQueryBuilder().build(), 0));
        }
    }

    @Test
    @DisplayName("generate key factories aware of tenancy")
    void testGenerateKeyFactory() {
        DatastoreWrapper wrapper = wrap(localDatastore(), multitenant());
        String tenantId1 = "first-tenant-ID";
        String tenantId1Prefixed = "Vfirst-tenant-ID";
        String tenantId2 = "second@tenant.id";
        String tenantId2Prefixed = "Esecond-at-tenant.id";
        String tenantId3 = "third.id";
        String tenantId3Prefixed = "Dthird.id";
        Datastore datastore = wrapper.datastore();
        ensureNamespace(tenantId1Prefixed, datastore);
        ensureNamespace(tenantId2Prefixed, datastore);
        ensureNamespace(tenantId3Prefixed, datastore);
        EmailAddress emailAddress2 = EmailAddress
                .newBuilder()
                .setValue(tenantId2)
                .vBuild();
        InternetDomain internetDomain3 = InternetDomain
                .newBuilder()
                .setValue(tenantId3)
                .vBuild();
        TenantId id1 = TenantId.newBuilder()
                               .setValue(tenantId1)
                               .vBuild();
        TenantId id2 = TenantId.newBuilder()
                               .setEmail(emailAddress2)
                               .vBuild();
        TenantId id3 = TenantId.newBuilder()
                               .setDomain(internetDomain3)
                               .vBuild();
        checkTenantIdInKey(tenantId1Prefixed, id1, wrapper);
        checkTenantIdInKey(tenantId2Prefixed, id2, wrapper);
        checkTenantIdInKey(tenantId3Prefixed, id3, wrapper);
    }

    @Test
    @DisplayName("produce lazy iterator on query read")
    void testLazyIterator() {
        DatastoreWrapper wrapper = wrap(localDatastore(), singleTenant());
        int count = 2;
        Map<?, Entity> entities = newTestEntities(count, wrapper);
        Collection<Entity> expctedEntities = entities.values();
        wrapper.createOrUpdate(expctedEntities);

        StructuredQuery<Entity> query = newEntityQueryBuilder()
                .setKind(GENERIC_ENTITY_KIND.value())
                .build();
        Iterator<Entity> result = wrapper.read(query);

        assertTrue(result.hasNext());
        Entity first = result.next();
        assertTrue(result.hasNext());
        Entity second = result.next();

        assertThat(expctedEntities).contains(first);
        assertThat(expctedEntities).contains(second);

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
        DatastoreWrapper wrapper = wrap(localDatastore(), multitenant());
        TenantId tenantId = TenantId
                .newBuilder()
                .setValue("Luke_I_am_your_tenant.")
                .vBuild();
        String key = "noooooo";
        Key entityKey = new TenantAwareFunction0<Key>(tenantId) {
            @Override
            public Key apply() {
                Key entityKey = wrapper.keyFactory(Kind.of(NAMESPACE_HOLDER_KIND))
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

    /**
     * Cannot be moved to test environment because it uses package-local
     * {@link Entities#fromMessage(com.google.protobuf.Message, Key) fromMessage()}.
     */
    private static Map<Key, Entity> newTestEntities(int n, DatastoreWrapper wrapper) {
        Map<Key, Entity> result = new HashMap<>(n);
        for (int i = 0; i < n; i++) {
            Any message = Any.getDefaultInstance();
            Key key = newKey(format("record-%s", i), wrapper);
            Entity entity = Entities.fromMessage(message, key);
            result.put(key, entity);
        }
        return result;
    }

    /**
     * Cannot be moved to test environment because it uses package-local {@link RecordId}.
     */
    private static Key newKey(String id, DatastoreWrapper wrapper) {
        RecordId recordId = new RecordId(id);
        return wrapper.keyFor(GENERIC_ENTITY_KIND, recordId);
    }
}
