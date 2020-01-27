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

import com.google.cloud.datastore.Key;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.IterableSubject;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.spine.base.EntityState;
import io.spine.client.CompositeFilter;
import io.spine.client.IdFilter;
import io.spine.client.ResponseFormat;
import io.spine.client.TargetFilters;
import io.spine.core.Version;
import io.spine.protobuf.AnyPacker;
import io.spine.server.ContextSpec;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.entity.storage.ColumnName;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.LifecycleFlagField;
import io.spine.server.storage.RecordReadRequest;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageTest;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.given.CollegeEntity;
import io.spine.server.storage.datastore.given.CountingDatastoreWrapper;
import io.spine.server.storage.datastore.given.DsRecordStorageTestEnv;
import io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.EntityWithoutLifecycle;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.test.storage.Task;
import io.spine.testing.SlowTest;
import io.spine.testing.server.storage.datastore.SpyStorageFactory;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.truth.Truth.assertThat;
import static com.google.protobuf.util.Timestamps.toSeconds;
import static io.spine.client.Filters.all;
import static io.spine.client.Filters.either;
import static io.spine.client.Filters.eq;
import static io.spine.client.Filters.gt;
import static io.spine.client.Filters.lt;
import static io.spine.client.OrderBy.Direction.ASCENDING;
import static io.spine.json.Json.toCompactJson;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.ContextSpec.singleTenant;
import static io.spine.server.entity.FieldMasks.applyMask;
import static io.spine.server.entity.storage.EntityRecordWithColumns.create;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.ADMISSION_DEADLINE;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.CREATED;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.NAME;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.PASSING_GRADE;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.STATE_SPONSORED;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.STUDENT_COUNT;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.UNORDERED_COLLEGE_NAMES;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.ascendingBy;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.assertSortedBooleans;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.createAndStoreEntities;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.createAndStoreEntity;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.datastoreFactory;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.descendingBy;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.emptyFilters;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.emptyIdFilter;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.getStateSponsoredValues;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.masked;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.newCollegeId;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.newEntityRecord;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.newIdFilter;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.newTargetFilters;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.orderBy;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.orderedAndLimited;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.recordIds;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.sortedIds;
import static io.spine.server.storage.datastore.given.TestEnvironment.singleTenantSpec;
import static io.spine.test.storage.Project.Status.STARTED;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("`DsRecordStorage` should")
class DsRecordStorageTest extends RecordStorageTest<DsRecordStorage<ProjectId>> {

    private final TestDatastoreStorageFactory datastoreFactory = datastoreFactory();

    @SuppressWarnings("unchecked") // OK for tests.
    @Override
    protected DsRecordStorage<ProjectId> newStorage(Class<? extends Entity<?, ?>> entityClass) {
        Class<? extends Entity<ProjectId, ?>> cls =
                (Class<? extends Entity<ProjectId, ?>>) entityClass;
        return (DsRecordStorage<ProjectId>)
                datastoreFactory.createRecordStorage(singleTenantSpec(), cls);
    }

    @Override
    protected Class<TestCounterEntity> getTestEntityClass() {
        return TestCounterEntity.class;
    }

    @Override
    protected EntityState newState(ProjectId projectId) {
        Project project = Project
                .newBuilder()
                .setId(projectId)
                .setName("Some test name")
                .addTask(Task.getDefaultInstance())
                .setStatus(Project.Status.CREATED)
                .vBuild();
        return project;
    }

    private EntityRecordWithColumns newRecordWithColumns(RecordStorage<ProjectId> storage) {
        EntityRecord record = newStorageRecord();
        Entity<ProjectId, Project> entity = new TestCounterEntity(newId());
        EntityRecordWithColumns recordWithColumns = create(record, entity, storage);
        return recordWithColumns;
    }

    @BeforeEach
    void setUp() {
        datastoreFactory.setUp();
    }

    @AfterEach
    void tearDown() {
        datastoreFactory.tearDown();
    }

    @Test
    @DisplayName("provide an access to `DatastoreWrapper` for extensibility")
    void testAccessDatastoreWrapper() {
        DsRecordStorage<ProjectId> storage = storage();
        DatastoreWrapper datastore = storage.getDatastore();
        assertNotNull(datastore);
    }

    @Test
    @DisplayName("provide an access to `TypeUrl` for extensibility")
    void testAccessTypeUrl() {
        DsRecordStorage<ProjectId> storage = storage();
        TypeUrl typeUrl = storage.getTypeUrl();
        assertNotNull(typeUrl);

        // According to the `TestConstCounterEntity` declaration.
        assertEquals(TypeUrl.of(Project.class), typeUrl);
    }

    @SuppressWarnings({
            "OverlyLongMethod",
            // A complicated test case verifying right Datastore behavior on
            // a low level of DatastoreWrapper and Datastore Entity.
            // Additionally checks the standard predefined Datastore Column Types
            "ProtoTimestampGetSecondsGetNano"
            // Compares points in time field-by-field.
    })
    @Test
    @DisplayName("persist entity columns beside the corresponding record")
    void testPersistColumns() {
        String projectStatusValue = "project_status_value";
        String internal = "internal";
        String projectVersion = "project_version";
        String dueDate = "due_date";
        String wrappedState = "wrapped_state";
        String archived = LifecycleFlagField.archived.name();
        String deleted = LifecycleFlagField.deleted.name();

        ProjectId id = newId();
        TestCounterEntity entity = new TestCounterEntity(id);
        entity.assignStatus(STARTED);
        Project state = entity.state();
        Version versionValue = entity.version();
        EntityRecord record = EntityRecord
                .newBuilder()
                .setState(pack(state))
                .setEntityId(pack(id))
                .setVersion(versionValue)
                .vBuild();
        DsRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
        EntityRecordWithColumns recordWithColumns = create(record, entity, storage);
        ImmutableSet<String> columns = recordWithColumns.columnNames()
                                                        .stream()
                                                        .map(ColumnName::value)
                                                        .collect(toImmutableSet());
        assertNotNull(columns);

        IterableSubject assertColumns = assertThat(columns);

        // Custom Columns
        assertColumns.containsAtLeast(projectStatusValue,
                                      internal,
                                      projectVersion,
                                      dueDate,
                                      wrappedState);
        // Columns defined in superclasses
        assertColumns.containsAtLeast(archived, deleted);

        // High level write operation
        storage.write(id, recordWithColumns);

        // Read Datastore Entity
        DatastoreWrapper datastore = storage.getDatastore();
        Key key = datastore.keyFor(
                Kind.of(state),
                RecordId.ofEntityId(id));
        com.google.cloud.datastore.Entity datastoreEntity = datastore.read(key);

        // Check entity record
        TypeUrl recordType = TypeUrl.from(EntityRecord.getDescriptor());
        EntityRecord readRecord = Entities.toMessage(datastoreEntity, recordType);
        assertEquals(record, readRecord);

        // Check custom Columns
        assertEquals(entity.getProjectStatusValue(), datastoreEntity.getLong(projectStatusValue));
        assertEquals(entity.getProjectVersion()
                           .getNumber(), datastoreEntity.getLong(projectVersion));

        com.google.cloud.Timestamp actualDueDate =
                datastoreEntity.getTimestamp(dueDate);
        assertEquals(toSeconds(entity.getDueDate()),
                     actualDueDate.getSeconds());
        assertEquals(entity.getDueDate()
                           .getNanos(),
                     actualDueDate.getNanos());
        assertEquals(entity.getInternal(), datastoreEntity.getBoolean(internal));
        assertEquals(toCompactJson(entity.getWrappedState()),
                     datastoreEntity.getString(wrappedState));

        // Check standard Columns
        assertEquals(entity.isArchived(), datastoreEntity.getBoolean(archived));
        assertEquals(entity.isDeleted(), datastoreEntity.getBoolean(deleted));
    }

    @SlowTest
    @Test
    @DisplayName("pass big data speed test")
    void testBigData() {
        // Default bulk size is 500 records - the maximum records that could be written within
        // one write operation
        long maxReadTime = 1000;
        long maxWriteTime = 9500;

        DsRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);

        BigDataTester.<ProjectId>newBuilder()
                .setEntryFactory(new BigDataTester.EntryFactory<ProjectId>() {
                    @Override
                    public ProjectId newId() {
                        return DsRecordStorageTest.this.newId();
                    }

                    @Override
                    public EntityRecordWithColumns newRecord() {
                        return DsRecordStorageTest.this.newRecordWithColumns(storage);
                    }
                })
                .setReadLimit(maxReadTime)
                .setWriteLimit(maxWriteTime)
                .build()
                .testBigDataOperations(storage);
    }

    @Test
    @DisplayName("write and read records with lifecycle flags by ID")
    void testLifecycleFlags() {
        ProjectId id = newId();
        LifecycleFlags lifecycle = LifecycleFlags
                .newBuilder()
                .setArchived(true)
                .vBuild();
        EntityRecord record = EntityRecord
                .newBuilder()
                .setState(pack(newState(id)))
                .setLifecycleFlags(lifecycle)
                .setEntityId(pack(id))
                .vBuild();
        TestCounterEntity entity = new TestCounterEntity(id);
        RecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
        EntityRecordWithColumns recordWithColumns = create(record, entity, storage);
        storage.write(id, recordWithColumns);

        RecordReadRequest<ProjectId> request = new RecordReadRequest<>(id);
        Optional<EntityRecord> restoredRecordOptional = storage.read(request);

        assertTrue(restoredRecordOptional.isPresent());

        // Includes Lifecycle flags comparison
        EntityRecord restoredRecord = restoredRecordOptional.get();
        assertEquals(record, restoredRecord);
    }

    @Nested
    @DisplayName("lookup `Datastore` records by IDs")
    class LookupByIds {

        private final ContextSpec contextSpec = singleTenant(LookupByIds.class.getName());
        private RecordStorage<CollegeId> storage;
        private CountingDatastoreWrapper datastoreWrapper;

        @BeforeEach
        void setUp() {
            datastoreWrapper = new CountingDatastoreWrapper(datastoreFactory().datastore(), false);
            SpyStorageFactory.injectWrapper(datastoreWrapper);
            StorageFactory storageFactory = new SpyStorageFactory();
            storage = storageFactory.createRecordStorage(contextSpec, CollegeEntity.class);
        }

        @Disabled
        @Test
        @DisplayName("returning proper entity")
        void testQueryByIDs() {
            // Create 10 entities and pick one for tests.
            int recordCount = 10;
            int targetEntityIndex = 7;
            List<CollegeEntity> entities = createAndStoreEntities(storage, recordCount);
            CollegeEntity targetEntity = entities.get(targetEntityIndex);

            // Create ID filter.
            Any targetId = pack(targetEntity.id());
            IdFilter idFilter = newIdFilter(targetId);

            // Create column filter.
            Timestamp targetColumnValue = targetEntity.getCreated();
            CompositeFilter columnFilter = all(eq(CREATED.columnName(), targetColumnValue));

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter, columnFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery =
                    EntityQueries.from(entityFilters, storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery);

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertThat(resultList)
                    .hasSize(1);

            // Check the record state.
            EntityRecord record = resultList.get(0);
            assertThat(unpack(record.getState()))
                    .isEqualTo(targetEntity.state());

            assertDsReadByKeys();
        }

        @Test
        @DisplayName("in descending sort order")
        void testQueryByIDsWithDescendingOrder() {
            // Create entities.
            int recordCount = UNORDERED_COLLEGE_NAMES.size();
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES);

            // Create ID filter.
            List<Any> targetIds = idsAsAny(entities);
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters,
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, descendingBy(NAME));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> expectedResults = reverse(sortedIds(entities, CollegeEntity::getName));
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);

            assertDsReadByKeys();
        }

        @Test
        @DisplayName("in an order set by a `String` field")
        void testQueryByIDsWithOrderByString() {
            testOrdering(NAME, CollegeEntity::getName);
        }

        @Test
        @DisplayName("in order set by `double` field")
        void testQueryByIDsWithOrderByDouble() {
            testOrdering(PASSING_GRADE, CollegeEntity::getPassingGrade);
        }

        @Test
        @DisplayName("in order set by a `Timestamp` field")
        void testQueryByIDsWithOrderByTimestamp() {
            testOrdering(ADMISSION_DEADLINE, entity -> entity.getAdmissionDeadline()
                                                             .getSeconds());
        }

        @Test
        @DisplayName("in an order set by an `Integer` field")
        void testQueryByIDsWithOrderByInt() {
            testOrdering(STUDENT_COUNT, CollegeEntity::getStudentCount);
        }

        /**
         * Uses local {@link SpyStorageFactory} so cannot be moved to test environment.
         */
        private <T extends Comparable<T>> void
        testOrdering(CollegeEntity.CollegeColumn column, Function<CollegeEntity, T> property) {
            // Create entities.
            int expectedRecordCount = UNORDERED_COLLEGE_NAMES.size() - 2;
            List<CollegeEntity> entities =
                    createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES)
                            .subList(0, expectedRecordCount);

            // Create ID filter.
            List<Any> targetIds = idsAsAny(entities);
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters,
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, ascendingBy(column));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(expectedRecordCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> expectedResults = sortedIds(entities, property);
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);
            assertDsReadByKeys();
        }

        private List<Any> idsAsAny(List<CollegeEntity> entities) {
            return entities.stream()
                           .map(Entity::id)
                           .map(AnyPacker::pack)
                           .collect(toList());
        }

        @Test
        @DisplayName("in an order set by a `boolean` field")
        void testQueryByIDsWithOrderByBoolean() {
            // Create entities.
            int recordCount = 20;
            List<CollegeEntity> entities = createAndStoreEntities(storage, recordCount);

            // Create ID filter.
            List<Any> targetIds = idsAsAny(entities);
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters,
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult =
                    storage.readAll(entityQuery, ascendingBy(STATE_SPONSORED));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<Boolean> actualResults = getStateSponsoredValues(resultList);
            assertSortedBooleans(actualResults);

            assertDsReadByKeys();
        }

        @Test
        @DisplayName("in specified order with missing entities")
        void testQueryByIDsWithOrderWithMissingEntities() {
            // Create entities.
            int recordCount = 12;
            List<CollegeEntity> entities = createAndStoreEntities(storage, recordCount);

            // Create ID filter.
            List<Any> targetIds = idsAsAny(entities);
            targetIds.add(2, pack(newCollegeId()));
            targetIds.add(5, pack(newCollegeId()));
            targetIds.add(7, pack(newCollegeId()));
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters,
                                                                    storage);

            // Execute Query.k
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery,
                                                                ascendingBy(STUDENT_COUNT));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> expectedResults = sortedIds(entities, CollegeEntity::getStudentCount);
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);

            assertDsReadByKeys();
        }

        @Test
        @DisplayName("a specified number of entities")
        void testQueryByIDsWithLimit() {
            // Create entities.
            int expectedCount = 4;
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES);

            // Create ID filter.
            List<Any> targetIds = idsAsAny(entities);
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters, storage);

            ResponseFormat format = orderedAndLimited(orderBy(NAME, ASCENDING), expectedCount);
            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, format);

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(expectedCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> sortedIds = sortedIds(entities, CollegeEntity::getName);
            List<CollegeId> expectedResults = sortedIds.subList(0, expectedCount);
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);

            assertDsReadByKeys();
        }

        @Test
        @DisplayName("for entities without lifecycle")
        void testQueryEntityWithoutLifecycleById() {
            DsRecordStorage<ProjectId> storage = newStorage(EntityWithoutLifecycle.class);
            ProjectId id = newId();
            EntityRecord record = newEntityRecord(id, newState(id));
            EntityWithoutLifecycle entity = new EntityWithoutLifecycle(id);
            storage.writeRecord(entity.id(), create(record, entity, storage));

            // Create ID filter.
            List<Any> targetIds = singletonList(pack(entity.id()));
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<ProjectId> entityQuery = EntityQueries.from(entityFilters,
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery);
            assertEquals(record, readResult.next());
            assertFalse(readResult.hasNext());
        }

        private void assertDsReadByKeys() {
            assertThat(datastoreWrapper.readByKeysCount())
                    .isEqualTo(1);
            assertThat(datastoreWrapper.readByQueryCount())
                    .isEqualTo(0);
        }
    }

    /**
     * Overrides and disables test from parent: {@link RecordStorageTest#rewritingExisting()}.
     */
    @Test
    @DisplayName("given bulk of records, write them re-writing existing ones")
    void rewritingExisting() {
    }

    @Nested
    @DisplayName("lookup records in Datastore by columns")
    class LookupByQueries {

        private final ContextSpec contextSpec = singleTenant(LookupByQueries.class.getName());
        private RecordStorage<CollegeId> storage;
        private CountingDatastoreWrapper datastoreWrapper;

        @BeforeEach
        void setUp() {
            datastoreWrapper = new CountingDatastoreWrapper(datastoreFactory().datastore(), false);
            SpyStorageFactory.injectWrapper(datastoreWrapper);
            StorageFactory storageFactory = new SpyStorageFactory();
            storage = storageFactory.createRecordStorage(contextSpec, CollegeEntity.class);
        }

        @Test
        @DisplayName("returning proper entity for single column")
        void testQueryByColumn() {
            // Create 10 entities and pick one for tests.
            int recordCount = 10;
            int targetEntityIndex = 7;
            List<CollegeEntity> entities = createAndStoreEntities(storage, recordCount);
            CollegeEntity targetEntity = entities.get(targetEntityIndex);

            // Create column filter.
            String targetColumnValue = targetEntity.getName();
            CompositeFilter columnFilter = all(eq(NAME.columnName(), targetColumnValue));

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(emptyIdFilter(),
                                                           columnFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters, storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery);

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(1, resultList.size());

            // Check the record state.
            EntityRecord record = resultList.get(0);
            assertEquals(targetEntity.state(), unpack(record.getState()));

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("returning proper entity for multiple columns")
        void testQueryByMultipleColumns() {
            // Create 10 entities and pick one for tests.
            int recordCount = 10;
            int targetEntityIndex = 7;
            List<CollegeEntity> entities = createAndStoreEntities(storage, recordCount);
            CollegeEntity targetEntity = entities.get(targetEntityIndex);

            // Create column filter.
            CompositeFilter columnFilter = all(
                    eq(NAME.columnName(), targetEntity.getName()),
                    eq(CREATED.columnName(), targetEntity.getCreated())
            );
            TargetFilters entityFilters = newTargetFilters(emptyIdFilter(),
                                                           columnFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters, storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery);

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(1, resultList.size());

            // Check the record state.
            EntityRecord record = resultList.get(0);
            assertEquals(targetEntity.state(), unpack(record.getState()));

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("with masked state")
        void testFieldMaskApplied() {
            // Create 10 entities and pick one for tests.
            int recordCount = 10;
            int targetEntityIndex = 7;
            List<CollegeEntity> entities = createAndStoreEntities(storage, recordCount);
            CollegeEntity targetEntity = entities.get(targetEntityIndex);

            // Create column filter.
            CompositeFilter columnFilter = all(
                    eq(NAME.columnName(), targetEntity.getName()),
                    eq(CREATED.columnName(), targetEntity.getCreated())
            );
            TargetFilters entityFilters = newTargetFilters(emptyIdFilter(),
                                                           columnFilter);

            // Compose Query.
            EntityQuery<CollegeId> query = EntityQueries.from(entityFilters, storage);

            // Execute Query.
            FieldMask mask = DsRecordStorageTestEnv.newFieldMask("id", "name");
            Iterator<EntityRecord> readResult = storage.readAll(query, masked(mask));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(1, resultList.size());

            // Check the record state.
            EntityRecord record = resultList.get(0);
            College expectedState = applyMask(mask, targetEntity.state());
            College actualState = (College) unpack(record.getState());
            assertNotEquals(targetEntity.state(), actualState);
            assertEquals(expectedState, actualState);

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("in descending sort order")
        void testQueryWithDescendingOrder() {
            // Create entities.
            int expectedRecordCount = UNORDERED_COLLEGE_NAMES.size();
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES);

            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(), storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, descendingBy(NAME));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(expectedRecordCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> expectedResults = reverse(sortedIds(entities, CollegeEntity::getName));
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("in an order set by a `String` field")
        void testQueryWithOrderByString() {
            testOrdering(NAME, CollegeEntity::getName);
        }

        @Test
        @DisplayName("in order set by a `double` field")
        void testQueryWithOrderByDouble() {
            testOrdering(PASSING_GRADE, CollegeEntity::getPassingGrade);
        }

        @Test
        @DisplayName("in order set by a `Timestamp` field")
        void testQueryWithOrderByTimestamp() {
            testOrdering(ADMISSION_DEADLINE, entity -> entity.getAdmissionDeadline()
                                                             .getSeconds());
        }

        @Test
        @DisplayName("in an order set by an `Integer` field")
        void testQueryWithOrderByInt() {
            testOrdering(STUDENT_COUNT, CollegeEntity::getStudentCount);
        }

        /**
         * Uses local {@link SpyStorageFactory} so cannot be moved to test environment.
         */
        private <T extends Comparable<T>> void
        testOrdering(CollegeEntity.CollegeColumn column, Function<CollegeEntity, T> property) {
            // Create entities.
            int recordCount = UNORDERED_COLLEGE_NAMES.size();
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(), storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, ascendingBy(column));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> expectedResults = sortedIds(entities, property);
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);
            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("in an order set by a `boolean` field")
        void testQueryWithOrderByBoolean() {
            // Create entities.
            int recordCount = 20;
            createAndStoreEntities(storage, recordCount);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(), storage);

            // Execute Query.
            Iterator<EntityRecord> readResult =
                    storage.readAll(entityQuery, ascendingBy(STATE_SPONSORED));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<Boolean> actualResults = getStateSponsoredValues(resultList);
            assertSortedBooleans(actualResults);

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("a specified number of entities")
        void testQueryWithLimit() {
            // Create entities.
            int expectedCount = 4;
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(), storage);
            ResponseFormat format = orderedAndLimited(orderBy(NAME, ASCENDING), expectedCount);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, format);

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(expectedCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> sortedIds = sortedIds(entities, CollegeEntity::getName);
            List<CollegeId> expectedResults = sortedIds.subList(0, expectedCount);
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);

            assertDsReadByStructuredQuery();
        }

        @SlowTest
        @Test
        @DisplayName("with multiple Datastore reads")
        void performsMultipleReads() {
            createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES, 300, false);
            createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES, 250, true);
            createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES, 150, false);
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES,
                                                                  150, true);
            TargetFilters filters = TargetFilters
                    .newBuilder()
                    .addFilter(either(
                            lt(NAME.columnName(), UNORDERED_COLLEGE_NAMES.get(2)),
                            gt(NAME.columnName(), UNORDERED_COLLEGE_NAMES.get(2))
                    ))
                    .addFilter(all(
                            eq(STATE_SPONSORED.columnName(), true),
                            eq(STUDENT_COUNT.columnName(), 150)
                    ))
                    .vBuild();
            int recordCount = 5;
            EntityQuery<CollegeId> query = EntityQueries.from(filters, storage);
            ResponseFormat format = orderedAndLimited(orderBy(NAME, ASCENDING), recordCount);
            Iterator<EntityRecord> recordIterator = storage.readAll(query, format);
            List<EntityRecord> resultList = newArrayList(recordIterator);

            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            entities.remove(2);
            List<CollegeId> expectedResults = sortedIds(entities, CollegeEntity::getName);
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);
            assertDsReadByStructuredQuery(2);
        }

        @Test
        @DisplayName("returning single entity for multiple `EITHER` filter matches")
        void testEitherFilterDuplicates() {
            CollegeEntity entity = createAndStoreEntity(storage);

            // Create `EITHER` column filter.
            int randomStudentCount = 15;
            CompositeFilter eitherFilter = either(
                    eq(NAME.columnName(), entity.getName()),
                    eq(STUDENT_COUNT.columnName(), randomStudentCount),
                    eq(PASSING_GRADE.columnName(), entity.getPassingGrade())
            );
            TargetFilters entityFilters = newTargetFilters(emptyIdFilter(), eitherFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters, storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery);

            // Check the entity is "found" only once.
            List<EntityRecord> foundEntities = newArrayList(readResult);
            assertEquals(1, foundEntities.size());

            // Check it's the target entity.
            EntityRecord record = foundEntities.get(0);
            assertEquals(entity.state(), unpack(record.getState()));

            // Check there was a correct amount of Datastore reads.
            assertDsReadByStructuredQuery(3);
        }

        private void assertDsReadByStructuredQuery() {
            assertDsReadByStructuredQuery(1);
        }

        private void assertDsReadByStructuredQuery(int invocationCount) {
            assertThat(datastoreWrapper.readByKeysCount())
                    .isEqualTo(0);
            assertThat(datastoreWrapper.readByQueryCount())
                    .isEqualTo(invocationCount);
        }
    }
}
