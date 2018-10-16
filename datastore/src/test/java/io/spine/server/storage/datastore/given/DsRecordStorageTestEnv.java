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

package io.spine.server.storage.datastore.given;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.client.CompositeColumnFilter;
import io.spine.client.EntityFilters;
import io.spine.client.EntityId;
import io.spine.client.EntityIdFilter;
import io.spine.client.EntityIdFilterVBuilder;
import io.spine.client.OrderBy;
import io.spine.client.OrderByVBuilder;
import io.spine.client.Pagination;
import io.spine.client.PaginationVBuilder;
import io.spine.core.Version;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.AbstractVersionableEntity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.datastore.TestDatastoreStorageFactory;
import io.spine.server.storage.given.RecordStorageTestEnv;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.test.datastore.CollegeIdVBuilder;
import io.spine.test.datastore.CollegeVBuilder;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.validate.TimestampVBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.asList;
import static io.spine.base.Identifier.newUuid;
import static io.spine.base.Time.getCurrentTime;
import static io.spine.client.OrderBy.Direction.ASCENDING;
import static io.spine.client.OrderBy.Direction.DESCENDING;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.entity.storage.EntityRecordWithColumns.create;
import static java.lang.Math.abs;
import static java.util.Collections.unmodifiableList;
import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A test environment for {@link io.spine.server.storage.datastore.DsRecordStorageTest}
 *
 * @author Mykhailo Drachuk
 */
public class DsRecordStorageTestEnv {

    public static final String COLUMN_NAME_FOR_STORING = "columnName";
    public static final ImmutableList<String> UNORDERED_COLLEGE_NAMES = ImmutableList.of(
            "Ivy University", "Doonesbury", "Winston University", "Springfield A&M",
            "Greendale Community College", "Monsters University"
    );
    private static final Random RANDOM = new SecureRandom();
    private static final long MAX_TIMESTAMP_SECONDS = 250000000000L;

    /**
     * Prevents instantiation of this test environment.
     */
    private DsRecordStorageTestEnv() {
    }

    public static TestDatastoreStorageFactory datastoreFactory() {
        return TestDatastoreStorageFactory.defaultInstance();
    }

    public static Pagination emptyPagination() {
        return Pagination.getDefaultInstance();
    }

    public static OrderBy emptyOrderBy() {
        return OrderBy.getDefaultInstance();
    }

    public static FieldMask emptyFieldMask() {
        return FieldMask.getDefaultInstance();
    }

    public static EntityFilters newEntityFilters(EntityIdFilter idFilter) {
        return EntityFilters.newBuilder()
                            .setIdFilter(idFilter)
                            .build();
    }

    public static EntityFilters newEntityFilters(EntityIdFilter idFilter,
                                                 CompositeColumnFilter columnFilter) {
        return EntityFilters.newBuilder()
                            .setIdFilter(idFilter)
                            .addFilter(columnFilter)
                            .build();
    }

    public static EntityIdFilter newIdFilter(EntityId firstId, EntityId... otherIds) {
        return newIdFilter(asList(firstId, otherIds));
    }

    public static EntityIdFilter newIdFilter(List<EntityId> targetIds) {
        return EntityIdFilterVBuilder.newBuilder()
                                     .addAllIds(targetIds)
                                     .build();
    }

    public static EntityId
    newEntityId(AbstractEntity<? extends Message, ? extends Message> targetEntity) {
        return EntityId.newBuilder()
                       .setId(pack(targetEntity.getId()))
                       .build();
    }

    public static List<EntityId>
    newEntityIds(Collection<CollegeEntity> targetEntities) {
        return targetEntities.stream()
                             .map(DsRecordStorageTestEnv::newEntityId)
                             .collect(toList());
    }

    public static void storeEntity(RecordStorage<CollegeId> storage, CollegeEntity entity) {
        EntityRecord record = newEntityRecord(entity.getId(), entity.getState());
        EntityRecordWithColumns withColumns = create(record, entity, storage);
        storage.write(entity.getId(), withColumns);
    }

    public static EntityRecord newEntityRecord(Message id, Message state) {
        return EntityRecord.newBuilder()
                           .setEntityId(pack(id))
                           .setState(pack(state))
                           .build();
    }

    public static OrderBy ascendingBy(String column) {
        return orderBy(column, ASCENDING);
    }

    public static OrderBy descendingBy(String column) {
        return orderBy(column, DESCENDING);
    }

    private static OrderBy orderBy(String column, OrderBy.Direction descending) {
        return OrderByVBuilder.newBuilder()
                              .setColumn(column)
                              .setDirection(descending)
                              .build();
    }

    public static List<CollegeId> recordIds(List<EntityRecord> resultList) {
        return resultList.stream()
                         .map(EntityRecord::getEntityId)
                         .map(AnyPacker::unpack)
                         .map(id -> (CollegeId) id)
                         .collect(toList());
    }

    public static <T extends Comparable<T>> List<CollegeId>
    sortedIds(List<CollegeEntity> entities, Function<CollegeEntity, T> property) {
        return entities.stream()
                       .sorted(comparing(property, nullsFirst(naturalOrder())))
                       .map(AbstractEntity::getId)
                       .collect(toList());
    }

    public static <T extends Comparable<T>> List<T>
    sortedValues(List<CollegeEntity> entities, Function<CollegeEntity, T> property) {
        return entities.stream()
                       .sorted(comparing(property, nullsFirst(naturalOrder())))
                       .map(property)
                       .collect(toList());
    }

    public static Pagination pagination(int pageSize) {
        return PaginationVBuilder.newBuilder()
                                 .setPageSize(pageSize)
                                 .build();
    }

    public static List<CollegeEntity>
    createAndStoreEntities(RecordStorage<CollegeId> storage, int recordCount) {
        List<CollegeEntity> entities = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            CollegeEntity entity = createAndStoreEntity(storage);
            entities.add(entity);
        }
        return entities;
    }

    public static List<CollegeEntity>
    createAndStoreEntitiesWithNullStudentCount(RecordStorage<CollegeId> storage, int recordCount) {
        List<CollegeEntity> entities = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            CollegeEntity entity = createAndStoreEntity(storage);
            entities.add(entity);
        }
        return entities;
    }

    public static List<CollegeEntity>
    createAndStoreEntities(RecordStorage<CollegeId> storage, List<String> names) {
        return names.stream()
                    .map(name -> createAndStoreEntity(storage, name))
                    .collect(toList());
    }

    private static CollegeEntity createAndStoreEntity(RecordStorage<CollegeId> storage) {
        CollegeId id = newId();
        CollegeEntity entity = new CollegeEntity(id);
        entity.injectState(newCollege(id));
        storeEntity(storage, entity);
        return entity;
    }

    private static CollegeEntity createAndStoreEntity(RecordStorage<CollegeId> storage,
                                                      String name) {
        CollegeId id = newId();
        CollegeEntity entity = new CollegeEntity(id);
        entity.injectState(newCollege(id, name, false));
        storeEntity(storage, entity);
        return entity;
    }

    private static College newCollege(CollegeId id, String name, boolean nullStudentCount) {
        return CollegeVBuilder.newBuilder()
                              .setId(id)
                              .setName(name)
                              .setAdmissionDeadline(randomTimestamp())
                              .setPassingGrade(randomPassingGrade())
                              .setStudentCount(nullStudentCount ? 0 : randomStudentCount())
                              .setStateSponsored(RANDOM.nextBoolean())
                              .build();
    }

    private static College newCollege(CollegeId id) {
        return newCollege(id, id.getValue(), false);
    }

    private static College newCollege(CollegeId id, boolean nullStudentCount) {
        return newCollege(id, id.getValue(), nullStudentCount);
    }

    private static int randomStudentCount() {
        return RANDOM.nextInt(1000);
    }

    private static double randomPassingGrade() {
        return RANDOM.nextDouble() * 9 + 1;
    }

    private static Timestamp randomTimestamp() {
        return TimestampVBuilder.newBuilder()
                                .setSeconds(abs(RANDOM.nextLong()) % MAX_TIMESTAMP_SECONDS)
                                .build();
    }

    private static CollegeId newId() {
        return CollegeIdVBuilder.newBuilder()
                                .setValue(newUuid())
                                .build();
    }

    public static void assertSortedBooleans(List<Boolean> values) {
        int boolSwitches = 0;
        boolean lastBool = false;
        for (boolean value : values) {
            if (lastBool != value) {
                boolSwitches++;
            }
            lastBool = value;
        }
        assertEquals(1, boolSwitches);
    }

    public static List<Boolean> getStateSponsoredValues(List<EntityRecord> resultList) {
        return resultList.stream()
                         .map(EntityRecord::getState)
                         .map(state -> (College) unpack(state))
                         .map(College::getStateSponsored)
                         .collect(toList());
    }

    public static List<Integer> nullableStudentCount(List<EntityRecord> resultList) {
        return resultList.stream()
                         .map(EntityRecord::getState)
                         .map(state -> (College) unpack(state))
                         .map(College::getStudentCount)
                         .map(count -> count == 0 ? null : count)
                         .collect(toList());
    }

    public static List<CollegeEntity> combine(Collection<CollegeEntity> nullEntities,
                                              Collection<CollegeEntity> regularEntities) {
        Stream<CollegeEntity> combination = concat(nullEntities.stream(), regularEntities.stream());
        return unmodifiableList(combination.collect(toList()));
    }

    /*
     * Test Entity types
     ************************/

    public static class TestEntity extends RecordStorageTestEnv.TestCounterEntity {

        protected TestEntity(ProjectId id) {
            super(id);
        }
    }

    public static class EntityWithCustomColumnName extends AbstractEntity<ProjectId, Any> {

        public EntityWithCustomColumnName(ProjectId id) {
            super(id);
        }

        @Column(name = COLUMN_NAME_FOR_STORING)
        public int getValue() {
            return 0;
        }
    }

    @SuppressWarnings("unused") // Reflective access
    public static class TestConstCounterEntity
            extends AbstractVersionableEntity<ProjectId, Project> {

        public static final String CREATED_COLUMN = "creationTime";
        public static final String COUNTER_ID_COLUMN = "counterName";
        private static final int COUNTER = 42;

        private final Timestamp creationTime;
        private LifecycleFlags lifecycleFlags;

        public TestConstCounterEntity(ProjectId id) {
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

        public void injectState(Project state, Version version) {
            updateState(state);
        }

        public void injectLifecycle(LifecycleFlags flags) {
            this.lifecycleFlags = flags;
        }
    }

    @SuppressWarnings("unused") // Reflective access
    public static class CollegeEntity
            extends AbstractVersionableEntity<CollegeId, College> {

        private final Timestamp creationTime;

        public CollegeEntity(CollegeId id) {
            super(id);
            this.creationTime = getCurrentTime();
        }

        @Column
        public String getName() {
            return getState().getName();
        }

        @Column
        public @Nullable Integer getStudentCount() {
            int count = getState().getStudentCount();
            return count == 0 ? null : count;
        }

        @Column
        public Timestamp getAdmissionDeadline() {
            return getState().getAdmissionDeadline();
        }

        @Column
        public double getPassingGrade() {
            return getState().getPassingGrade();
        }

        @Column
        public boolean getStateSponsored() {
            return getState().getStateSponsored();
        }

        @Column
        public Timestamp getCreationTime() {
            return creationTime;
        }

        private void injectState(College state) {
            updateState(state);
        }

        public enum Columns {
            CREATED("creationTime"),
            NAME("name"),
            STUDENT_COUNT("studentCount"),
            PASSING_GRADE("passingGrade"),
            ADMISSION_DEADLINE("admissionDeadline"),
            STATE_SPONSORED("stateSponsored");

            private final String name;

            Columns(String name) {
                this.name = name;
            }

            public String columnName() {
                return name;
            }
        }
    }
}
