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
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.StorageFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("InstanceMethodNamingConvention")
public class DatastoreStorageFactoryShould {

    private static final DatastoreOptions DUMMY_OPTIONS = DatastoreOptions.newBuilder()
            .setProjectId("dummy-dataset")
            .build();

    private static final Datastore DATASTORE = DUMMY_OPTIONS.getService();

    private static final StorageFactory FACTORY = DatastoreStorageFactory.newInstance(DATASTORE);

    @Test
    public void create_multitenant_storages() throws Exception {
        final StorageFactory factory = DatastoreStorageFactory.newInstance(DATASTORE, true);
        assertTrue(factory.isMultitenant());
        final CommandStorage storage = factory.createCommandStorage();
        assertTrue(storage.isMultitenant());
        storage.close();
    }

    @Test
    public void create_entity_storage_using_class_parameter() {
        final RecordStorage<String> storage = FACTORY.createRecordStorage(TestEntity.class);
        assertNotNull(storage);
    }

    @Test
    public void create_event_storage() {
        final EventStorage storage = FACTORY.createEventStorage();
        assertNotNull(storage);
    }

    @Test
    public void create_command_storage() {
        final CommandStorage storage = FACTORY.createCommandStorage();
        assertNotNull(storage);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void create_aggregate_storage_not_using_class_parameter() {
        final AggregateStorage<AggregateStorageRecord> storage = FACTORY.createAggregateStorage(null);
        assertNotNull(storage);
    }

    private static class TestEntity extends Entity<String, StringValue> {

        private TestEntity(String id) {
            super(id);
        }
    }
}
