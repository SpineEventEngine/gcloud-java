/*
 * Copyright 2018, TeamDev Ltd. All rights reserved.
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
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.base.Optional;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.Identifier;
import io.spine.client.CompositeColumnFilter;
import io.spine.client.EntityFilters;
import io.spine.client.EntityId;
import io.spine.client.EntityIdFilter;
import io.spine.core.Version;
import io.spine.core.Versions;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.AbstractVersionableEntity;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.EntityColumnCache;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordReadRequest;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageShould;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.test.storage.Task;
import io.spine.type.TypeUrl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.protobuf.util.Timestamps.toSeconds;
import static io.spine.client.ColumnFilters.all;
import static io.spine.client.ColumnFilters.eq;
import static io.spine.json.Json.toCompactJson;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.entity.storage.EntityQueries.from;
import static io.spine.server.entity.storage.EntityRecordWithColumns.create;
import static io.spine.test.Verify.assertContains;
import static io.spine.base.Time.getCurrentTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Dashenkov
 */
public class DsRecordStorageShould extends RecordStorageShould<ProjectId,
        DsRecordStorage<ProjectId>> {

    private static final String COLUMN_NAME_FOR_STORING = "columnName";
    private static final TestDatastoreStorageFactory datastoreFactory
            = TestDatastoreStorageFactory.getDefaultInstance();

    /**
     * Some tests create records from {@link TestConstCounterEntity} instead of {@link TestEntity} stored
     * in the {@linkplain DsRecordStorage storage}. So instead of {@linkplain RecordStorage#getEntityColumnCache()
     * obtaining} entity column cache from the {@linkplain DsRecordStorage storage}, we need to create a separate one.
     */
    private static final EntityColumnCache testCounterEntityColumnCache =
            EntityColumnCache.initializeFor(TestConstCounterEntity.class);

    @SuppressWarnings("unchecked") // OK for tests.
    @Override
    protected DsRecordStorage<ProjectId> newStorage(Class<? extends Entity> entityClass) {
        final Class<? extends Entity<ProjectId, ?>> cls =
                (Class<? extends Entity<ProjectId, ?>>) entityClass;
        return (DsRecordStorage<ProjectId>) datastoreFactory.createRecordStorage(cls);
    }

    @Override
    protected Class<? extends TestCounterEntity> getTestEntityClass() {
        return TestEntity.class;
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
        final EntityRecordWithColumns recordWithColumns = create(record, entity, testCounterEntityColumnCache);
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
        final String creationTime = TestConstCounterEntity.CREATED_COLUMN_NAME;
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
                                                .setState(pack(state))
                                                .setEntityId(pack(id))
                                                .setVersion(versionValue)
                                                .build();
        final EntityRecordWithColumns recordWithColumns = create(record, entity, testCounterEntityColumnCache);
        final Collection<String> columns = recordWithColumns.getColumnNames();
        assertNotNull(columns);

        // Custom Columns
        assertContains(counter, columns);
        assertContains(bigCounter, columns);
        assertContains(counterEven, columns);
        assertContains(counterVersion, columns);
        assertContains(creationTime, columns);
        assertContains(counterState, columns);

        // Columns defined in superclasses
        assertContains(version, columns);
        assertContains(archived, columns);
        assertContains(deleted, columns);

        final DsRecordStorage<ProjectId> storage = getStorage();

        // High level write operation
        storage.write(id, recordWithColumns);

        // Read Datastore Entity
        final DatastoreWrapper datastore = storage.getDatastore();
        final Key key = DsIdentifiers.keyFor(datastore,
                                             Kind.of(state),
                                             DsIdentifiers.ofEntityId(id));
        final com.google.cloud.datastore.Entity datastoreEntity = datastore.read(key);

        // Check entity record
        final TypeUrl recordType = TypeUrl.from(EntityRecord.getDescriptor());
        final EntityRecord readRecord = Entities.entityToMessage(datastoreEntity, recordType);
        assertEquals(record, readRecord);

        // Check custom Columns
        assertEquals(entity.getCounter(), datastoreEntity.getLong(counter));
        assertEquals(entity.getBigCounter(), datastoreEntity.getLong(bigCounter));
        assertEquals(entity.getCounterVersion()
                           .getNumber(), datastoreEntity.getLong(counterVersion));

        final com.google.cloud.Timestamp actualCreationTime =
                datastoreEntity.getTimestamp(creationTime);
        assertEquals(toSeconds(entity.getCreationTime()), actualCreationTime.getSeconds());
        assertEquals(entity.getCreationTime()
                           .getNanos(), actualCreationTime.getNanos());
        assertEquals(entity.isCounterEven(), datastoreEntity.getBoolean(counterEven));
        assertEquals(toCompactJson(entity.getCounterState()),
                     datastoreEntity.getString(counterState));

        // Check standard Columns
        assertEquals(entity.getVersion()
                           .getNumber(), datastoreEntity.getLong(version));
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
        final LifecycleFlags lifecycle = LifecycleFlags.newBuilder()
                                                       .setArchived(true)
                                                       .build();
        final EntityRecord record = EntityRecord.newBuilder()
                                                .setState(pack(newState(id)))
                                                .setLifecycleFlags(lifecycle)
                                                .setEntityId(pack(id))
                                                .build();
        final TestConstCounterEntity entity = new TestConstCounterEntity(id);
        entity.injectLifecycle(lifecycle);
        final EntityRecordWithColumns recordWithColumns = create(record, entity, testCounterEntityColumnCache);
        final RecordStorage<ProjectId> storage = getStorage();
        storage.write(id, recordWithColumns);
        final RecordReadRequest<ProjectId> request = new RecordReadRequest<>(id);
        final Optional<EntityRecord> restoredRecordOptional = storage.read(request);
        assertTrue(restoredRecordOptional.isPresent());
        final EntityRecord restoredRecord = restoredRecordOptional.get();
        // Includes Lifecycle flags comparison
        assertEquals(record, restoredRecord);
    }

    @Test
    public void convert_entity_record_to_entity_using_column_name_for_storing() {
        final DsRecordStorage<ProjectId> storage = newStorage(EntityWithCustomColumnName.class);
        final ProjectId id = newId();
        final EntityRecord record = EntityRecord.newBuilder()
                                                .setState(pack(newState(id)))
                                                .build();
        final Entity entity = new EntityWithCustomColumnName(id);
        final EntityRecordWithColumns entityRecordWithColumns = create(record, entity, storage.getEntityColumnCache());
        final com.google.cloud.datastore.Entity datastoreEntity =
                storage.entityRecordToEntity(id, entityRecordWithColumns);
        final Set<String> propertiesName = datastoreEntity.getNames();
        assertTrue(propertiesName.contains(COLUMN_NAME_FOR_STORING));
    }

    @Test
    public void query_by_IDs_when_possible() {
        SpyStorageFactory.injectWrapper(datastoreFactory.getDatastore());
        final DatastoreStorageFactory storageFactory = new SpyStorageFactory();
        final RecordStorage<ProjectId> storage =
                storageFactory.createRecordStorage(TestConstCounterEntity.class);
        final int recordCount = 10;
        final int targetEntityIndex = 7;
        final List<TestConstCounterEntity> entities = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            final TestConstCounterEntity entity = new TestConstCounterEntity(newId());
            entities.add(entity);
            final EntityRecord record = EntityRecord.newBuilder()
                                                    .setState(pack(entity.getState()))
                                                    .build();
            final EntityRecordWithColumns withColumns = create(record, entity, storage.getEntityColumnCache());
            storage.write(entity.getId(), withColumns);
        }
        final TestConstCounterEntity targetEntity = entities.get(targetEntityIndex);
        final EntityId targetId = EntityId.newBuilder()
                                          .setId(pack(targetEntity.getId()))
                                          .build();
        final Object columnTargetValue = targetEntity.getCreationTime();
        final EntityIdFilter idFilter = EntityIdFilter.newBuilder()
                                                      .addIds(targetId)
                                                      .build();
        final CompositeColumnFilter columnFilter =
                all(eq(TestConstCounterEntity.CREATED_COLUMN_NAME, columnTargetValue));
        final EntityFilters entityFilters = EntityFilters.newBuilder()
                                                         .setIdFilter(idFilter)
                                                         .addFilter(columnFilter)
                                                         .build();
        final EntityQuery<ProjectId> entityQuery = from(entityFilters,
                                                        storage.getEntityColumnCache());
        final Iterator<EntityRecord> readResult = storage.readAll(entityQuery,
                                                                  FieldMask.getDefaultInstance());
        final List<EntityRecord> resultList = newArrayList(readResult);
        assertEquals(1, resultList.size());
        assertEquals(targetEntity.getState(), unpack(resultList.get(0).getState()));

        final DatastoreWrapper spy = storageFactory.getDatastore();
        verify(spy).read(ArgumentMatchers.<Key>anyIterable());
        verify(spy, never()).read(any(StructuredQuery.class));
    }

    /*
     * Test Entity types
     ************************/

    @SuppressWarnings("unused") // Reflective access
    public static class TestConstCounterEntity
            extends AbstractVersionableEntity<ProjectId, Project> {

        private static final String CREATED_COLUMN_NAME = "creationTime";
        private static final int COUNTER = 42;

        private final Timestamp creationTime;
        private LifecycleFlags lifecycleFlags;

        protected TestConstCounterEntity(ProjectId id) {
            super(id);
            this.creationTime = getCurrentTime();
        }

        @Column
        public int getCounter() {
            return COUNTER;
        }

        @Column
        public long getBigCounter() {
            return getCounter();
        }

        @Column
        public boolean isCounterEven() {
            return true;
        }

        @Column
        public String getCounterName() {
            return getId().toString();
        }

        @Column
        public Version getCounterVersion() {
            return Version.newBuilder()
                          .setNumber(COUNTER)
                          .build();
        }

        @Column
        public Timestamp getCreationTime() {
            return creationTime;
        }

        @Column
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

    public static class TestEntity extends TestCounterEntity<ProjectId> {

        protected TestEntity(ProjectId id) {
            super(id);
        }
    }

    public static class EntityWithCustomColumnName extends AbstractEntity<ProjectId, Any> {
        private EntityWithCustomColumnName(ProjectId id) {
            super(id);
        }

        @Column(name = COLUMN_NAME_FOR_STORING)
        public int getValue() {
            return 0;
        }
    }

    /**
     * A {@link TestDatastoreStorageFactory} which spies on its {@link DatastoreWrapper}.
     */
    private static class SpyStorageFactory extends TestDatastoreStorageFactory {

        private static DatastoreWrapper spyWrapper = null;

        private static void injectWrapper(DatastoreWrapper wrapper) {
            spyWrapper = spy(wrapper);
        }

        private SpyStorageFactory() {
            super(spyWrapper.getDatastore());
        }

        @Override
        protected DatastoreWrapper createDatastoreWrapper(Datastore datastore) {
            return spyWrapper;
        }
    }
}
