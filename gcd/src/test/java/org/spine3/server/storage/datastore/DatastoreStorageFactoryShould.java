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

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.protobuf.StringValue;
import org.junit.Test;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.entity.AbstractEntity;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.ColumnType;
import org.spine3.server.entity.storage.ColumnTypeRegistry;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.StorageFactory;
import org.spine3.server.storage.datastore.type.DatastoreTypeRegistry;
import org.spine3.server.storage.datastore.type.SimpleDatastoreColumnType;
import org.spine3.test.aggregate.ProjectId;
import org.spine3.test.storage.Project;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("InstanceMethodNamingConvention")
public class DatastoreStorageFactoryShould {

    private static final DatastoreOptions DUMMY_OPTIONS = DatastoreOptions.newBuilder()
                                                                          .setProjectId("dummy-dataset")
                                                                          .build();

    private static final Datastore datastore = DUMMY_OPTIONS.getService();

    private static final StorageFactory datastoreFactory = DatastoreStorageFactory.newBuilder()
                                                                                  .setDatastore(datastore)
                                                                                  .build();

    @Test
    public void create_multitenant_storages() throws Exception {
        final StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                              .setDatastore(datastore)
                                                              .setMultitenant(true)
                                                              .build();
        assertTrue(factory.isMultitenant());
        final RecordStorage storage = factory.createStandStorage();
        assertTrue(storage.isMultitenant());
        storage.close();
    }

    @Test
    public void construct_with_default_type_registry() {
        final StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                              .setDatastore(datastore)
                                                              .build();
        final ColumnTypeRegistry registry = factory.getTypeRegistry();
        assertNotNull(registry);
    }

    @Test
    public void construct_with_extended_type_registry() {
        final StorageFactory factory =
                DatastoreStorageFactory.newBuilder()
                                       .setDatastore(datastore)
                                       .setTypeRegistry(DatastoreTypeRegistry.predefinedValuesAnd()
                                                                             .put(Byte.class, new MockByteColumnType())
                                                                             .build())
                                       .build();
        final ColumnTypeRegistry<?> registry = factory.getTypeRegistry();
        assertNotNull(registry);
        final ColumnType type = registry.get(mockColumn(Byte.class));
        assertNotNull(type);
    }

    @Test
    public void create_entity_storage_using_class_parameter() {
        final RecordStorage<String> storage = datastoreFactory.createRecordStorage(TestEntity.class);
        assertNotNull(storage);
    }

    @Test
    public void create_separate_record_storage_per_state_type() {
        final DsRecordStorage<?> storage = (DsRecordStorage<?>) datastoreFactory.createRecordStorage(TestEntity.class);
        final DsRecordStorage<?> differentStorage =
                (DsRecordStorage<?>) datastoreFactory.createRecordStorage(DifferentTestEntity.class);
        assertNotEquals(storage.getKind(), differentStorage.getKind());
    }

    @Test
    public void convert_itself_to_single_tenant() {
        final StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                              .setDatastore(datastore)
                                                              .setMultitenant(true)
                                                              .build();
        assertTrue(factory.isMultitenant());
        final StorageFactory singleTenantFactory = factory.toSingleTenant();
        assertFalse(singleTenantFactory.isMultitenant());
    }

    @Test
    public void return_self_if_single_tenant() {
        final StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                              .setDatastore(datastore)
                                                              .setMultitenant(false)
                                                              .build();
        assertFalse(factory.isMultitenant());
        final StorageFactory singleTenantFactory = factory.toSingleTenant();
        assertFalse(singleTenantFactory.isMultitenant());
        assertSame(factory, singleTenantFactory);
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void fail_to_create_aggregate_storage_not_using_class_parameter() {
        final AggregateStorage<ProjectId> storage = datastoreFactory.createAggregateStorage(null);
        assertNotNull(storage);
    }

    @Test
    public void have_default_column_type_registry() {
        final DatastoreStorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                                       .setDatastore(datastore)
                                                                       .build();
        final ColumnTypeRegistry defaultRegistry = factory.getTypeRegistry();
        assertNotNull(defaultRegistry);
        assertSame(DatastoreTypeRegistry.defaultInstance(), defaultRegistry);
    }

    @Test(expected = NullPointerException.class)
    public void fail_to_construct_without_datastore() {
        DatastoreStorageFactory.newBuilder()
                               .build();
    }

    private static <T> Column<T> mockColumn(Class<T> type) {
        @SuppressWarnings("unchecked")
        final Column<T> mock = mock(Column.class);
        when(mock.getType()).thenReturn(type);
        return mock;
    }

    private static class TestEntity extends AbstractEntity<String, StringValue> {

        private TestEntity(String id) {
            super(id);
        }
    }

    private static class DifferentTestEntity extends AbstractEntity<String, Project> {
        protected DifferentTestEntity(String id) {
            super(id);
        }
    }

    private static class MockByteColumnType extends SimpleDatastoreColumnType<Byte> {
        @Override
        public void setColumnValue(BaseEntity.Builder storageRecord, Byte value, String columnIdentifier) {
            // NOP
        }
    }
}
