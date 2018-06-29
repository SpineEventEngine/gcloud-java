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
import com.google.cloud.datastore.DatastoreOptions;
import com.google.protobuf.StringValue;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import io.spine.test.storage.Project;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("InstanceMethodNamingConvention")
public class DatastoreStorageFactoryShould {

    private static final DatastoreOptions DUMMY_OPTIONS =
            DatastoreOptions.newBuilder()
                            .setProjectId("dummy-dataset")
                            .build();

    private static final Datastore datastore = DUMMY_OPTIONS.getService();

    private static final StorageFactory datastoreFactory =
            DatastoreStorageFactory.newBuilder()
                                   .setDatastore(datastore)
                                   .build();

    @Test
    public void create_multitenant_storages() {
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
    public void create_entity_storage_using_class_parameter() {
        final RecordStorage<String> storage = datastoreFactory.createRecordStorage(TestEntity.class);
        assertNotNull(storage);
    }

    @Test
    public void create_separate_record_storage_per_state_type() {
        final DsRecordStorage<?> storage =
                (DsRecordStorage<?>) datastoreFactory.createRecordStorage(TestEntity.class);
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
    @Test
    public void fail_to_create_aggregate_storage_not_using_class_parameter() {
        assertThrows(NullPointerException.class,
                     () -> datastoreFactory.createAggregateStorage(null));
    }

    @Test
    public void have_default_column_type_registry() {
        final DatastoreStorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                                       .setDatastore(datastore)
                                                                       .build();
        final ColumnTypeRegistry defaultRegistry = factory.getTypeRegistry();
        assertNotNull(defaultRegistry);
        assertSame(DatastoreTypeRegistryFactory.defaultInstance(), defaultRegistry);
    }

    @Test
    public void fail_to_construct_without_datastore() {
        assertThrows(NullPointerException.class, DatastoreStorageFactory.newBuilder()::build);
    }

    @Test
    public void do_nothing_on_close() throws Exception {
        final DatastoreStorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                                       .setDatastore(datastore)
                                                                       .build();
        factory.close();
        // Multiple calls are allowed as no action is performed
        factory.close();
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
}
