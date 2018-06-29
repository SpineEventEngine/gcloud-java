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
import io.spine.server.datastore.TestEnvironment;
import io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers;
import io.spine.server.tenant.TenantAwareFunction0;
import io.spine.server.tenant.TenantAwareOperation;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.collect.Lists.newArrayList;
import static io.spine.server.storage.datastore.TestDatastoreWrapper.wrap;
import static io.spine.server.storage.datastore.given.Given.testProjectId;
import static io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers.multitenant;
import static io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers.singleTenant;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class DatastoreWrapperShould {

    private static final String NAMESPACE_HOLDER_KIND = "spine.test.NAMESPACE_HOLDER_KIND";

    @AfterClass
    public static void tearDown() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               singleTenant());
        wrapper.dropTable(NAMESPACE_HOLDER_KIND);
    }

    @Test
    public void work_with_transactions_if_necessary() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               singleTenant());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.commitTransaction();
        assertFalse(wrapper.isTransactionActive());
    }

    @Test
    public void rollback_transactions() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               singleTenant());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.rollbackTransaction();
        assertFalse(wrapper.isTransactionActive());
    }

    @Test
    public void fail_to_start_transaction_if_one_is_active() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               singleTenant());
        try {
            wrapper.startTransaction();
            assertTrue(wrapper.isTransactionActive());
            assertThrows(IllegalStateException.class, wrapper::startTransaction);
        } finally {
            wrapper.rollbackTransaction();
        }
    }

    @Test
    public void fail_to_finish_not_active_transaction() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore(),
                                                               singleTenant());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.commitTransaction();
        assertFalse(wrapper.isTransactionActive());
        assertThrows(IllegalStateException.class, wrapper::rollbackTransaction);
    }

    @Test
    public void support_big_bulk_ID_reads() throws InterruptedException {
        final int bulkSize = 1001;

        final TestDatastoreWrapper wrapper = wrap(Given.testDatastore(), false);
        final Map<Key, Entity> entities = Given.nEntities(bulkSize, wrapper);
        final Collection<Entity> expectedEntities = entities.values();

        wrapper.createOrUpdate(expectedEntities);

        // Wait for some time to make sure the writing is complete
        Thread.sleep(bulkSize * 5);

        final Collection<Entity> readEntities = newArrayList(wrapper.read(entities.keySet()));
        assertEquals(entities.size(), readEntities.size());
        assertTrue(expectedEntities.containsAll(readEntities));

        wrapper.dropAllTables();
    }

    @Ignore // This test rarely passes on Travis CI due to eventual consistency.
    @Test
    public void support_big_bulk_query_reads() throws InterruptedException {
        final int bulkSize = 2001;

        final TestDatastoreWrapper wrapper = wrap(Given.testDatastore(), false);
        final Map<Key, Entity> entities = Given.nEntities(bulkSize, wrapper);
        final Collection<Entity> expectedEntities = entities.values();

        wrapper.createOrUpdate(expectedEntities);

        // Wait for some time to make sure the writing is complete
        Thread.sleep(bulkSize * 3);

        final StructuredQuery<Entity> query = Query.newEntityQueryBuilder()
                                                   .setKind(Given.GENERIC_ENTITY_KIND.getValue())
                                                   .build();
        final Collection<Entity> readEntities = newArrayList(wrapper.read(query));
        assertEquals(entities.size(), readEntities.size());
        assertTrue(expectedEntities.containsAll(readEntities));

        wrapper.dropAllTables();
    }

    @Test
    public void generate_key_factories_aware_of_tenancy() {
        final ProjectId projectId = ProjectId.of(TestDatastoreStorageFactory.DEFAULT_DATASET_NAME);
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(
                Given.testDatastore(),
                TestNamespaceSuppliers.multitenant(projectId));
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

    @Test
    public void produce_lazy_iterator_on_query_read() {
        final DatastoreWrapper wrapper = wrap(Given.testDatastore(), singleTenant());
        final int count = 2;
        final Map<?, Entity> entities = Given.nEntities(count, wrapper);
        final Collection<Entity> expctedEntities = entities.values();
        wrapper.createOrUpdate(expctedEntities);

        final StructuredQuery<Entity> query = Query.newEntityQueryBuilder()
                                                   .setKind(Given.GENERIC_ENTITY_KIND.getValue())
                                                   .build();
        final Iterator<Entity> result = wrapper.read(query);

        assertTrue(result.hasNext());
        final Entity first = result.next();
        assertTrue(result.hasNext());
        final Entity second = result.next();

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
    public void allow_new_namespaces_on_go() {
        final DatastoreWrapper wrapper = wrap(Given.testDatastore(), multitenant(testProjectId()));
        final TenantId tenantId = TenantId.newBuilder()
                                          .setValue("Luke_I_am_your_tenant.")
                                          .build();
        final String key = "noooooo";
        final Key entityKey = new TenantAwareFunction0<Key>(tenantId) {
            @Override
            public Key apply() {
                final Key entityKey = wrapper.getKeyFactory(Kind.of(NAMESPACE_HOLDER_KIND))
                                             .newKey(key);
                final Entity entity = Entity.newBuilder(entityKey)
                                            .build();
                wrapper.create(entity);
                return entityKey;
            }
        }.execute();

        // Clean up the namespace.
        wrapper.delete(entityKey);
    }

    private static void checkTenantIdInKey(final String id, TenantId tenantId, final DatastoreWrapper wrapper) {
        new TenantAwareOperation(tenantId) {
            @Override
            public void run() {
                final Key key = wrapper.getKeyFactory(DatastoreWrapperShould.Given.GENERIC_ENTITY_KIND)
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
            final boolean onCi = TestEnvironment.runsOnCi();
            return onCi
                   ? TestDatastoreFactory.getTestRemoteDatastore()
                   : TestDatastoreFactory.getLocalDatastore();
        }

        private static Map<Key, Entity> nEntities(int n, DatastoreWrapper wrapper) {
            final Map<Key, Entity> result = new HashMap<>(n);
            for (int i = 0; i < n; i++) {
                final Any message = Any.getDefaultInstance();
                final RecordId recordId = new RecordId(String.format("record-%s", i));
                final Key key = DsIdentifiers.keyFor(wrapper, GENERIC_ENTITY_KIND, recordId);
                final Entity entity = Entities.messageToEntity(message, key);
                result.put(key, entity);
            }
            return result;
        }
    }
}
