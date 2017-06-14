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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.base.Optional;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.spine.base.Identifier;
import io.spine.base.Version;
import io.spine.base.Versions;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.AbstractVersionableEntity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageShould;
import io.spine.test.Tests;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.test.storage.Task;
import io.spine.type.TypeUrl;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static io.spine.json.Json.toCompactJson;
import static io.spine.server.entity.storage.EntityRecordWithColumns.create;
import static io.spine.test.Verify.assertContainsKey;
import static io.spine.time.Time.getCurrentTime;

/**
 * @author Dmytro Dashenkov
 */
public class DsRecordStorageShould extends RecordStorageShould<ProjectId,
                                                               DsRecordStorage<ProjectId>> {

    private static final TestDatastoreStorageFactory datastoreFactory
            = TestDatastoreStorageFactory.getDefaultInstance();

    @Override
    protected DsRecordStorage<ProjectId> getStorage() {
        return (DsRecordStorage<ProjectId>) datastoreFactory.createRecordStorage(
                TestConstCounterEntity.class);
    }

    @Override
    protected ProjectId newId() {
        final ProjectId projectId = ProjectId.newBuilder()
                                             .setId(Identifier.newUuid())
                                             .build();
        return projectId;
    }

    @Override
    protected Message newState(ProjectId projectId) {
        final Project project = Project.newBuilder()
                                       .setId(projectId)
                                       .setName("Some test name")
                                       .addTask(Task.getDefaultInstance())
                                       .setStatus(Project.Status.CREATED)
                                       .build();
        return project;
    }

    private EntityRecordWithColumns newRecordWithColumns() {
        final EntityRecord record = newStorageRecord();
        final TestConstCounterEntity entity = new TestConstCounterEntity(newId());
        final EntityRecordWithColumns recordWithColumns = create(record, entity);
        return recordWithColumns;
    }

    @Before
    public void setUp() throws Exception {
        datastoreFactory.setUp();
    }

    @After
    public void tearDown() throws Exception {
        datastoreFactory.tearDown();
    }

    @Test
    public void provide_access_to_DatastoreWrapper_for_extensibility() {
        final DsRecordStorage<ProjectId> storage = getStorage();
        final DatastoreWrapper datastore = storage.getDatastore();
        assertNotNull(datastore);
    }

    @Test
    public void provide_access_to_TypeUrl_for_extensibility() {
        final DsRecordStorage<ProjectId> storage = getStorage();
        final TypeUrl typeUrl = storage.getTypeUrl();
        assertNotNull(typeUrl);

        // According to the `TestConstCounterEntity` declaration.
        assertEquals(TypeUrl.of(Project.class), typeUrl);
    }

    @SuppressWarnings("OverlyLongMethod")
    // A complicated test case verifying right Datastore behavior on
    // a low level of DatastoreWrapper and Datastore Entity/
    // Additionally checks the standard predefined Datastore Column Types
    @Test
    public void persist_entity_Columns_beside_its_record() {
        final String counter = "counter";
        final String bigCounter = "bigCounter";
        final String counterEven = "counterEven";
        final String counterVersion = "counterVersion";
        final String creationTime = "creationTime";
        final String counterState = "counterState";
        final String version = "version";
        final String archived = "archived";
        final String deleted = "deleted";

        final ProjectId id = newId();
        final Project state = (Project) newState(id);
        final Version versionValue = Versions.newVersion(5, getCurrentTime());
        final TestConstCounterEntity entity = new TestConstCounterEntity(id);
        entity.injectState(state, versionValue);
        final EntityRecord record = EntityRecord.newBuilder()
                                                .setState(AnyPacker.pack(state))
                                                .setEntityId(AnyPacker.pack(id))
                                                .setVersion(versionValue)
                                                .build();
        final EntityRecordWithColumns recordWithColumns = create(record, entity);
        final Map<String, Column> columns = recordWithColumns.getColumns();
        assertNotNull(columns);

        // Custom Columns
        assertContainsKey(counter, columns);
        assertContainsKey(bigCounter, columns);
        assertContainsKey(counterEven, columns);
        assertContainsKey(counterVersion, columns);
        assertContainsKey(creationTime, columns);
        assertContainsKey(counterState, columns);

        // Columns defined in superclasses
        assertContainsKey(version, columns);
        assertContainsKey(archived, columns);
        assertContainsKey(deleted, columns);

        final DsRecordStorage<ProjectId> storage = getStorage();

        // High level write operation
        storage.write(id, recordWithColumns);

        // Read Datastore Entity
        final DatastoreWrapper datastore = storage.getDatastore();
        final Key key = DsIdentifiers.keyFor(datastore,
                                             Kind.of(state),
                                             DsIdentifiers.ofEntityId(id));
        final Entity datastoreEntity = datastore.read(key);

        // Check entity record
        final TypeUrl recordType = TypeUrl.from(EntityRecord.getDescriptor());
        final EntityRecord readRecord = Entities.entityToMessage(datastoreEntity, recordType);
        assertEquals(record, readRecord);

        // Check custom Columns
        assertEquals(entity.getCounter(), datastoreEntity.getLong(counter));
        assertEquals(entity.getBigCounter(), datastoreEntity.getLong(bigCounter));
        assertEquals(entity.getCounterVersion()
                           .getNumber(), datastoreEntity.getLong(counterVersion));

        assertEquals(Timestamps.toMicros(entity.getCreationTime()),
                     // in Datastore max DateTime precision is 1 microsecond
                     datastoreEntity.getDateTime(creationTime)
                                    .getTimestampMicroseconds());
        assertEquals(entity.isCounterEven(), datastoreEntity.getBoolean(counterEven));
        assertEquals(toCompactJson(entity.getCounterState()),
                     datastoreEntity.getString(counterState));

        // Check standard Columns
        assertEquals(entity.getVersion().getNumber(), datastoreEntity.getLong(version));
        assertEquals(entity.isArchived(), datastoreEntity.getBoolean(archived));
        assertEquals(entity.isDeleted(), datastoreEntity.getBoolean(deleted));
    }

    @Test
    public void pass_big_data_speed_test() {
        // Default bulk size is 500 records - the maximum records that could be written within
        // one write operation
        final long maxReadTime = 1000;
        final long maxWriteTime = 9500;

        BigDataTester.<ProjectId>newBuilder()
                .setEntryFactory(new BigDataTester.EntryFactory<ProjectId>() {
                    @Override
                    public ProjectId newId() {
                        return DsRecordStorageShould.this.newId();
                    }

                    @Override
                    public EntityRecordWithColumns newRecord() {
                        return DsRecordStorageShould.this.newRecordWithColumns();
                    }
                })
                .setReadLimit(maxReadTime)
                .setWriteLimit(maxWriteTime)
                .build()
                .testBigDataOperations(getStorage());
    }

    @Test
    public void write_and_read_records_with_lifecycle_flags() {
        final ProjectId id = newId();
        final LifecycleFlags lifecycle = Tests.archived();
        final EntityRecord record = EntityRecord.newBuilder()
                                                .setState(AnyPacker.pack(newState(id)))
                                                .setLifecycleFlags(lifecycle)
                                                .setEntityId(AnyPacker.pack(id))
                                                .build();
        final TestConstCounterEntity entity = new TestConstCounterEntity(id);
        entity.injectLifecycle(lifecycle);
        final EntityRecordWithColumns recordWithColumns = create(record, entity);
        final RecordStorage<ProjectId> storage = getStorage();
        storage.write(id, recordWithColumns);
        final Optional<EntityRecord> restoredRecordOptional = storage.read(id);
        assertTrue(restoredRecordOptional.isPresent());
        final EntityRecord restoredRecord = restoredRecordOptional.get();
        // Includes Lifecycle flags comparison
        assertEquals(record, restoredRecord);
    }

    @SuppressWarnings("unused") // Reflective access
    public static class TestConstCounterEntity
                  extends AbstractVersionableEntity<ProjectId, Project> {

        private static final int COUNTER = 42;
        private final Timestamp creationTime;
        private LifecycleFlags lifecycleFlags;

        protected TestConstCounterEntity(ProjectId id) {
            super(id);
            this.creationTime = getCurrentTime();
        }

        public int getCounter() {
            return COUNTER;
        }

        public long getBigCounter() {
            return getCounter();
        }

        public boolean isCounterEven() {
            return true;
        }

        public String getCounterName() {
            return getId().toString();
        }

        public Version getCounterVersion() {
            return Version.newBuilder()
                          .setNumber(COUNTER)
                          .build();
        }

        public Timestamp getCreationTime() {
            return creationTime;
        }

        public Project getCounterState() {
            return getState();
        }

        @Override
        public LifecycleFlags getLifecycleFlags() {
            return lifecycleFlags == null ? super.getLifecycleFlags() : lifecycleFlags;
        }

        private void injectState(Project state, Version version) {
            updateState(state);
        }

        private void injectLifecycle(LifecycleFlags flags) {
            this.lifecycleFlags = flags;
        }
    }
}