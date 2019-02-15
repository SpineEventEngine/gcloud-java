/*
 * Copyright 2019, TeamDev. All rights reserved.
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
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.truth.IterableSubject;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.client.CompositeFilter;
import io.spine.client.IdFilter;
import io.spine.client.TargetFilters;
import io.spine.core.Version;
import io.spine.core.Versions;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordReadRequest;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageTest;
import io.spine.server.storage.datastore.given.CollegeEntity;
import io.spine.server.storage.datastore.given.DsRecordStorageTestEnv;
import io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.EntityWithCustomColumnName;
import io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.TestEntity;
import io.spine.server.storage.datastore.given.TestConstCounterEntity;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.test.storage.Task;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.truth.Truth.assertThat;
import static com.google.protobuf.util.Timestamps.toSeconds;
import static io.spine.base.Time.getCurrentTime;
import static io.spine.client.Filters.all;
import static io.spine.client.Filters.either;
import static io.spine.client.Filters.eq;
import static io.spine.client.Filters.gt;
import static io.spine.client.Filters.lt;
import static io.spine.json.Json.toCompactJson;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.entity.FieldMasks.applyMask;
import static io.spine.server.entity.storage.EntityRecordWithColumns.create;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.ADMISSION_DEADLINE;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.CREATED;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.NAME;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.PASSING_GRADE;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.STATE_SPONSORED;
import static io.spine.server.storage.datastore.given.CollegeEntity.CollegeColumn.STUDENT_COUNT;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.COLUMN_NAME_FOR_STORING;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.UNORDERED_COLLEGE_NAMES;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.ascendingBy;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.assertSortedBooleans;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.combine;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.createAndStoreEntities;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.createAndStoreEntitiesWithNullStudentCount;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.createAndStoreEntity;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.datastoreFactory;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.descendingBy;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.emptyFieldMask;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.emptyFilters;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.emptyIdFilter;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.emptyOrderBy;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.emptyPagination;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.getStateSponsoredValues;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.newCollegeId;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.newEntityRecord;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.newIdFilter;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.newTargetFilters;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.nullableStudentCount;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.pagination;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.recordIds;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.sortedIds;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.sortedValues;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@DisplayName("DsRecordStorage should")
class DsRecordStorageTest extends RecordStorageTest<DsRecordStorage<ProjectId>> {

    private final TestDatastoreStorageFactory datastoreFactory = datastoreFactory();

    @SuppressWarnings("unchecked") // OK for tests.
    @Override
    protected DsRecordStorage<ProjectId> newStorage(Class<? extends Entity> entityClass) {
        Class<? extends Entity<ProjectId, ?>> cls =
                (Class<? extends Entity<ProjectId, ?>>) entityClass;
        return (DsRecordStorage<ProjectId>) datastoreFactory.createRecordStorage(cls);
    }

    @Override
    protected Class<? extends TestCounterEntity> getTestEntityClass() {
        return TestEntity.class;
    }

    @Override
    protected Message newState(ProjectId projectId) {
        Project project = Project
                .newBuilder()
                .setId(projectId)
                .setName("Some test name")
                .addTask(Task.getDefaultInstance())
                .setStatus(Project.Status.CREATED)
                .build();
        return project;
    }

    private EntityRecordWithColumns newRecordWithColumns(RecordStorage<ProjectId> storage) {
        EntityRecord record = newStorageRecord();
        Entity<ProjectId, Project> entity = TestConstCounterEntity.create(newId());
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
    @DisplayName("provide access to DatastoreWrapper for extensibility")
    void testAccessDatastoreWrapper() {
        DsRecordStorage<ProjectId> storage = getStorage();
        DatastoreWrapper datastore = storage.getDatastore();
        assertNotNull(datastore);
    }

    @Test
    @DisplayName("provide access to TypeUrl for extensibility")
    void testAccessTypeUrl() {
        DsRecordStorage<ProjectId> storage = getStorage();
        TypeUrl typeUrl = storage.getTypeUrl();
        assertNotNull(typeUrl);

        // According to the `TestConstCounterEntity` declaration.
        assertEquals(TypeUrl.of(Project.class), typeUrl);
    }

    @SuppressWarnings("OverlyLongMethod")
    // A complicated test case verifying right Datastore behavior on
    // a low level of DatastoreWrapper and Datastore Entity.
    // Additionally checks the standard predefined Datastore Column Types
    @Test
    @DisplayName("persist entity columns beside the corresponding record")
    void testPersistColumns() {
        String counter = "counter";
        String bigCounter = "bigCounter";
        String counterEven = "counterEven";
        String counterVersion = "counterVersion";
        @SuppressWarnings("DuplicateStringLiteralInspection") // common column name
                String creationTime = "creationTime";
        String counterState = "counterState";
        String version = "version";
        String archived = "archived";
        String deleted = "deleted";

        ProjectId id = newId();
        Project state = (Project) newState(id);
        Version versionValue = Versions.newVersion(5, getCurrentTime());
        TestConstCounterEntity entity = TestConstCounterEntity.create(id, state);
        EntityRecord record = EntityRecord.newBuilder()
                                          .setState(pack(state))
                                          .setEntityId(pack(id))
                                          .setVersion(versionValue)
                                          .build();
        DsRecordStorage<ProjectId> storage = newStorage(TestConstCounterEntity.class);
        EntityRecordWithColumns recordWithColumns = create(record, entity, storage);
        Collection<String> columns = recordWithColumns.getColumnNames();
        assertNotNull(columns);

        IterableSubject assertColumns = assertThat(columns);

        // Custom Columns
        assertColumns.contains(counter);
        assertColumns.contains(bigCounter);
        assertColumns.contains(counterEven);
        assertColumns.contains(counterVersion);
        assertColumns.contains(creationTime);
        assertColumns.contains(counterState);

        // Columns defined in superclasses
        assertColumns.contains(version);
        assertColumns.contains(archived);
        assertColumns.contains(deleted);

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
        assertEquals(entity.getCounter(), datastoreEntity.getLong(counter));
        assertEquals(entity.getBigCounter(), datastoreEntity.getLong(bigCounter));
        assertEquals(entity.getCounterVersion()
                           .getNumber(), datastoreEntity.getLong(counterVersion));

        com.google.cloud.Timestamp actualCreationTime =
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
    @DisplayName("pass big data speed test")
    void testBigData() {
        // Default bulk size is 500 records - the maximum records that could be written within
        // one write operation
        long maxReadTime = 1000;
        long maxWriteTime = 9500;

        DsRecordStorage<ProjectId> storage = newStorage(TestConstCounterEntity.class);

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
                .build();
        EntityRecord record = EntityRecord
                .newBuilder()
                .setState(pack(newState(id)))
                .setLifecycleFlags(lifecycle)
                .setEntityId(pack(id))
                .build();
        TestConstCounterEntity entity = TestConstCounterEntity.create(id);
        entity.injectLifecycle(lifecycle);
        RecordStorage<ProjectId> storage = newStorage(TestConstCounterEntity.class);
        EntityRecordWithColumns recordWithColumns = create(record, entity, storage);
        storage.write(id, recordWithColumns);

        RecordReadRequest<ProjectId> request = new RecordReadRequest<>(id);
        Optional<EntityRecord> restoredRecordOptional = storage.read(request);

        assertTrue(restoredRecordOptional.isPresent());

        // Includes Lifecycle flags comparison
        EntityRecord restoredRecord = restoredRecordOptional.get();
        assertEquals(record, restoredRecord);
    }

    @Test
    @DisplayName("convert entity record to entity using column name for storing")
    void testUseColumnStoreName() {
        DsRecordStorage<ProjectId> storage = newStorage(EntityWithCustomColumnName.class);
        ProjectId id = newId();
        EntityRecord record = newEntityRecord(id, newState(id));
        Entity entity = new EntityWithCustomColumnName(id);
        EntityRecordWithColumns entityRecordWithColumns = create(record, entity, storage);
        com.google.cloud.datastore.Entity datastoreEntity =
                storage.entityRecordToEntity(id, entityRecordWithColumns);
        Set<String> propertiesName = datastoreEntity.getNames();
        assertTrue(propertiesName.contains(COLUMN_NAME_FOR_STORING));
    }

    @Nested
    @DisplayName("lookup Datastore records by IDs")
    class LookupByIds {

        private DatastoreStorageFactory storageFactory;
        private RecordStorage<CollegeId> storage;

        @BeforeEach
        void setUp() {
            SpyStorageFactory.injectWrapper(datastoreFactory().getDatastore());
            storageFactory = new SpyStorageFactory();
            storage = storageFactory.createRecordStorage(CollegeEntity.class);
        }

        @Test
        @DisplayName("returning proper entity")
        void testQueryByIDs() {
            // Create 10 entities and pick one for tests.
            int recordCount = 10;
            int targetEntityIndex = 7;
            List<CollegeEntity> entities = createAndStoreEntities(storage, recordCount);
            CollegeEntity targetEntity = entities.get(targetEntityIndex);

            // Create ID filter.
            Any targetId = pack(targetEntity.getId());
            IdFilter idFilter = newIdFilter(targetId);

            // Create column filter.
            Timestamp targetColumnValue = targetEntity.getCreationTime();
            CompositeFilter columnFilter = all(eq(CREATED.columnName(), targetColumnValue));

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter, columnFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery =
                    EntityQueries.from(entityFilters, emptyOrderBy(), emptyPagination(), storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(1, resultList.size());

            // Check the record state.
            EntityRecord record = resultList.get(0);
            assertEquals(targetEntity.getState(), unpack(record.getState()));

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
                                                                    descendingBy(NAME),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

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
        @DisplayName("in an order specified by string field")
        void testQueryByIDsWithOrderByString() {
            testOrdering(NAME, CollegeEntity::getName);
        }

        @Test
        @DisplayName("in order specified by double field")
        void testQueryByIDsWithOrderByDouble() {
            testOrdering(PASSING_GRADE, CollegeEntity::getPassingGrade);
        }

        @Test
        @DisplayName("in order specified by timestamp field")
        void testQueryByIDsWithOrderByTimestamp() {
            testOrdering(ADMISSION_DEADLINE, entity -> entity.getAdmissionDeadline()
                                                             .getSeconds());
        }

        @Test
        @DisplayName("in an order specified by integer")
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
                                                                    ascendingBy(column),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

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
                           .map(Entity::getId)
                           .map(AnyPacker::pack)
                           .collect(toList());
        }

        @Test
        @DisplayName("in an order specified by boolean")
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
                                                                    ascendingBy(STATE_SPONSORED),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<Boolean> actualResults = getStateSponsoredValues(resultList);
            assertSortedBooleans(actualResults);

            assertDsReadByKeys();
        }

        @Test
        @DisplayName("in specified order with nulls")
        void testQueryByIDsWithOrderWithNulls() {
            // Create entities.
            int nullCount = 11;
            int regularCount = 37;
            int recordCount = regularCount + nullCount;
            List<CollegeEntity> nullEntities =
                    createAndStoreEntitiesWithNullStudentCount(storage, nullCount);
            List<CollegeEntity> regularEntities = createAndStoreEntities(storage, regularCount);

            List<CollegeEntity> entities = combine(nullEntities, regularEntities);

            // Create ID filter.
            List<Any> targetIds = idsAsAny(entities);
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters,
                                                                    ascendingBy(STUDENT_COUNT),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.k
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<Integer> expectedCounts = sortedValues(entities, CollegeEntity::getStudentCount);
            List<Integer> actualCounts = nullableStudentCount(resultList);
            assertEquals(expectedCounts, actualCounts);

            // Check Datastore reads are performed by keys but not using a structured query.
            DatastoreWrapper spy = storageFactory.getDatastore();
            verify(spy).read(anyIterable());
            //noinspection unchecked OK for a generic class assignment in tests.
            verify(spy, never()).read(any(StructuredQuery.class));
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
                                                                    ascendingBy(STUDENT_COUNT),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.k
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

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
            int expectedRecordCount = 4;
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES);

            // Create ID filter.
            List<Any> targetIds = idsAsAny(entities);
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(entityFilters,
                                                                    ascendingBy(NAME),
                                                                    pagination(expectedRecordCount),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(expectedRecordCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> sortedIds = sortedIds(entities, CollegeEntity::getName);
            List<CollegeId> expectedResults = sortedIds.subList(0, expectedRecordCount);
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);

            assertDsReadByKeys();
        }

        @Test
        @DisplayName("for entities without lifecycle")
        void testQueryEntityWithoutLifecycleById() {
            DsRecordStorage<ProjectId> storage = newStorage(EntityWithCustomColumnName.class);
            ProjectId id = newId();
            EntityRecord record = newEntityRecord(id, newState(id));
            EntityWithCustomColumnName entity = new EntityWithCustomColumnName(id);
            storage.writeRecord(entity.getId(), create(record, entity, storage));

            // Create ID filter.
            List<Any> targetIds = singletonList(pack(entity.getId()));
            IdFilter idFilter = newIdFilter(targetIds);

            // Compose Query filters.
            TargetFilters entityFilters = newTargetFilters(idFilter);

            // Compose Query.
            EntityQuery<ProjectId> entityQuery = EntityQueries.from(entityFilters,
                                                                    emptyOrderBy(),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());
            assertEquals(record, readResult.next());
            assertFalse(readResult.hasNext());
        }

        private void assertDsReadByKeys() {
            DatastoreWrapper spy = storageFactory.getDatastore();
            verify(spy).read(anyIterable());
            //noinspection unchecked OK for a generic class assignment in tests.
            verify(spy, never()).read(any(StructuredQuery.class));
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

        private DatastoreStorageFactory storageFactory;
        private RecordStorage<CollegeId> storage;

        @BeforeEach
        void setUp() {
            SpyStorageFactory.injectWrapper(datastoreFactory().getDatastore());
            storageFactory = new SpyStorageFactory();
            storage = storageFactory.createRecordStorage(CollegeEntity.class);
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
            EntityQuery<CollegeId> entityQuery =
                    EntityQueries.from(entityFilters, emptyOrderBy(), emptyPagination(), storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(1, resultList.size());

            // Check the record state.
            EntityRecord record = resultList.get(0);
            assertEquals(targetEntity.getState(), unpack(record.getState()));

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
                    eq(CREATED.columnName(), targetEntity.getCreationTime())
            );
            TargetFilters entityFilters = newTargetFilters(emptyIdFilter(),
                                                           columnFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery =
                    EntityQueries.from(entityFilters, emptyOrderBy(), emptyPagination(), storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(1, resultList.size());

            // Check the record state.
            EntityRecord record = resultList.get(0);
            assertEquals(targetEntity.getState(), unpack(record.getState()));

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
                    eq(CREATED.columnName(), targetEntity.getCreationTime())
            );
            TargetFilters entityFilters = newTargetFilters(emptyIdFilter(),
                                                           columnFilter);

            // Compose Query.
            EntityQuery<CollegeId> query =
                    EntityQueries.from(entityFilters, emptyOrderBy(), emptyPagination(), storage);

            // Execute Query.
            FieldMask mask = DsRecordStorageTestEnv.newFieldMask("id", "name");
            Iterator<EntityRecord> readResult = storage.readAll(query,
                                                                DsRecordStorageTestEnv.newFieldMask(
                                                                        "id", "name"));

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(1, resultList.size());

            // Check the record state.
            EntityRecord record = resultList.get(0);
            College expectedState = applyMask(mask, targetEntity.getState());
            College actualState = (College) unpack(record.getState());
            assertNotEquals(targetEntity.getState(), actualState);
            assertEquals(expectedState, actualState);

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("in descending sort order")
        void testQueryWithDescendingOrder() {
            // Create entities.
            int expectedRecordCount = UNORDERED_COLLEGE_NAMES.size();
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES);

            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(),
                                                                    descendingBy(NAME),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

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
        @DisplayName("in an order specified by string field")
        void testQueryWithOrderByString() {
            testOrdering(NAME, CollegeEntity::getName);
        }

        @Test
        @DisplayName("in order specified by double field")
        void testQueryWithOrderByDouble() {
            testOrdering(PASSING_GRADE, CollegeEntity::getPassingGrade);
        }

        @Test
        @DisplayName("in order specified by timestamp field")
        void testQueryWithOrderByTimestamp() {
            testOrdering(ADMISSION_DEADLINE, entity -> entity.getAdmissionDeadline()
                                                             .getSeconds());
        }

        @Test
        @DisplayName("in an order specified by integer")
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
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(),
                                                                    ascendingBy(column),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

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
        @DisplayName("in an order specified by boolean")
        void testQueryWithOrderByBoolean() {
            // Create entities.
            int recordCount = 20;
            createAndStoreEntities(storage, recordCount);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(),
                                                                    ascendingBy(STATE_SPONSORED),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<Boolean> actualResults = getStateSponsoredValues(resultList);
            assertSortedBooleans(actualResults);

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("in specified order with nulls")
        void testQueryWithOrderWithNulls() {
            // Create entities.
            int nullCount = 5;
            int regularCount = 12;
            int recordCount = regularCount + nullCount;
            List<CollegeEntity> nullEntities =
                    createAndStoreEntitiesWithNullStudentCount(storage, nullCount);
            List<CollegeEntity> regularEntities = createAndStoreEntities(storage, regularCount);

            List<CollegeEntity> entities = combine(nullEntities, regularEntities);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(),
                                                                    ascendingBy(STUDENT_COUNT),
                                                                    emptyPagination(),
                                                                    storage);

            // Execute Query.k
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(recordCount, resultList.size());

            // Check the entities were ordered.
            List<Integer> expectedCounts = sortedValues(entities, CollegeEntity::getStudentCount);
            List<Integer> actualCounts = nullableStudentCount(resultList);
            assertEquals(expectedCounts, actualCounts);

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("a specified number of entities")
        void testQueryWithLimit() {
            // Create entities.
            int expectedRecordCount = 4;
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery = EntityQueries.from(emptyFilters(),
                                                                    ascendingBy(NAME),
                                                                    pagination(expectedRecordCount),
                                                                    storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> resultList = newArrayList(readResult);
            assertEquals(expectedRecordCount, resultList.size());

            // Check the entities were ordered.
            List<CollegeId> sortedIds = sortedIds(entities, CollegeEntity::getName);
            List<CollegeId> expectedResults = sortedIds.subList(0, expectedRecordCount);
            List<CollegeId> actualResults = recordIds(resultList);
            assertEquals(expectedResults, actualResults);

            assertDsReadByStructuredQuery();
        }

        @Test
        @DisplayName("with multiple Datastore reads")
        void performsMultipleReads() {
            createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES, 300, false);
            createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES, 250, true);
            createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES, 150, false);
            List<CollegeEntity> entities = createAndStoreEntities(storage, UNORDERED_COLLEGE_NAMES,
                                                                  150, true);
            TargetFilters filters =
                    TargetFilters.newBuilder()
                                 .addFilter(either(
                                         lt(NAME.columnName(), UNORDERED_COLLEGE_NAMES.get(2)),
                                         gt(NAME.columnName(), UNORDERED_COLLEGE_NAMES.get(2))
                                 ))
                                 .addFilter(all(
                                         eq(STATE_SPONSORED.columnName(), true),
                                         eq(STUDENT_COUNT.columnName(), 150)
                                 ))
                                 .build();
            int recordCount = 5;
            EntityQuery<CollegeId> query = EntityQueries.from(filters, ascendingBy(NAME),
                                                              pagination(recordCount), storage);
            Iterator<EntityRecord> recordIterator = storage.readAll(query, emptyFieldMask());
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
            CompositeFilter eitherFilter = either(
                    eq(NAME.columnName(), entity.getName()),
                    eq(PASSING_GRADE.columnName(), entity.getPassingGrade())
            );
            TargetFilters entityFilters = newTargetFilters(emptyIdFilter(), eitherFilter);

            // Compose Query.
            EntityQuery<CollegeId> entityQuery =
                    EntityQueries.from(entityFilters, emptyOrderBy(), emptyPagination(), storage);

            // Execute Query.
            Iterator<EntityRecord> readResult = storage.readAll(entityQuery, emptyFieldMask());

            // Check the query results.
            List<EntityRecord> foundEntities = newArrayList(readResult);
            assertEquals(1, foundEntities.size());

            // Check the record state.
            EntityRecord record = foundEntities.get(0);
            assertEquals(entity.getState(), unpack(record.getState()));

            assertDsReadByStructuredQuery();
        }

        private void assertDsReadByStructuredQuery() {
            assertDsReadByStructuredQuery(1);
        }

        private void assertDsReadByStructuredQuery(int invocationCount) {
            DatastoreWrapper spy = storageFactory.getDatastore();
            verify(spy, never()).read(anyIterable());
            //noinspection unchecked OK for a generic class assignment in tests.
            verify(spy, times(invocationCount)).read(any(StructuredQuery.class));
        }
    }

    /**
     * A {@link TestDatastoreStorageFactory} which spies on its {@link DatastoreWrapper}.
     *
     * This class is not moved to the
     * {@linkplain io.spine.server.storage.datastore.given.DsRecordStorageTestEnv test environment}
     * because it uses package-private method of {@link DatastoreWrapper}.
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
        protected DatastoreWrapper createDatastoreWrapper(Builder builder) {
            return spyWrapper;
        }
    }
}
