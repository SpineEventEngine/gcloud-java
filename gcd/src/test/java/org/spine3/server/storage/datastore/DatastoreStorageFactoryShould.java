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
import com.google.cloud.datastore.DatastoreOptions;
import com.google.protobuf.StringValue;
import org.junit.Test;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.entity.AbstractEntity;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.StorageFactory;
import org.spine3.test.aggregate.ProjectId;
import org.spine3.test.storage.Project;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void fail_to_create_aggregate_storage_not_using_class_parameter() {
        final AggregateStorage<ProjectId> storage = datastoreFactory.createAggregateStorage(null);
        assertNotNull(storage);
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
