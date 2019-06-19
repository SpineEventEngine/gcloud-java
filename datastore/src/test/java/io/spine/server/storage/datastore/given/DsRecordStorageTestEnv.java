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

package io.spine.server.storage.datastore.given;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.client.CompositeFilter;
import io.spine.client.EntityId;
import io.spine.client.IdFilter;
import io.spine.client.OrderBy;
import io.spine.client.Pagination;
import io.spine.client.TargetFilters;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.datastore.TestDatastoreStorageFactory;
import io.spine.server.storage.given.RecordStorageTestEnv;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static com.google.common.collect.Lists.asList;
import static io.spine.base.Identifier.newUuid;
import static io.spine.client.OrderBy.Direction.ASCENDING;
import static io.spine.client.OrderBy.Direction.DESCENDING;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.entity.storage.EntityRecordWithColumns.create;
import static java.util.Collections.shuffle;
import static java.util.Collections.unmodifiableList;
import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A test environment for {@link io.spine.server.storage.datastore.DsRecordStorageTest}.
 */
public class DsRecordStorageTestEnv {

    public static final String COLUMN_NAME_FOR_STORING = "columnName";

    public static final ImmutableList<String> UNORDERED_COLLEGE_NAMES = ImmutableList.of(
            "Ivy University", "Doonesbury", "Winston University", "Springfield A&M",
            "Greendale Community College", "Monsters University"
    );

    private static final Random RANDOM = new SecureRandom();
    private static final int MAX_TIMESTAMP_SECONDS = 250000000;

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

    public static IdFilter emptyIdFilter() {
        return IdFilter.getDefaultInstance();
    }

    public static TargetFilters emptyFilters() {
        return TargetFilters.getDefaultInstance();
    }

    public static TargetFilters newTargetFilters(IdFilter idFilter) {
        return TargetFilters
                .newBuilder()
                .setIdFilter(idFilter)
                .vBuild();
    }

    public static TargetFilters newTargetFilters(IdFilter idFilter, CompositeFilter columnFilter) {
        return TargetFilters
                .newBuilder()
                .setIdFilter(idFilter)
                .addFilter(columnFilter)
                .vBuild();
    }

    public static IdFilter newIdFilter(Any firstId, Any... otherIds) {
        return newIdFilter(asList(firstId, otherIds));
    }

    public static IdFilter newIdFilter(List<Any> targetIds) {
        return IdFilter
                .newBuilder()
                .addAllId(targetIds)
                .vBuild();
    }

    public static EntityId
    extractEntityId(AbstractEntity<? extends Message, ? extends Message> targetEntity) {
        return EntityId
                .newBuilder()
                .setId(pack(targetEntity.id()))
                .vBuild();
    }

    public static List<EntityId>
    extractEntityIds(Collection<CollegeEntity> targetEntities) {
        return targetEntities
                .stream()
                .map(DsRecordStorageTestEnv::extractEntityId)
                .collect(toList());
    }

    public static EntityRecord newEntityRecord(Message id, Message state) {
        return EntityRecord
                .newBuilder()
                .setEntityId(pack(id))
                .setState(pack(state))
                .vBuild();
    }

    public static OrderBy ascendingBy(CollegeEntity.CollegeColumn column) {
        return orderBy(column.columnName(), ASCENDING);
    }

    public static OrderBy descendingBy(CollegeEntity.CollegeColumn column) {
        return orderBy(column.columnName(), DESCENDING);
    }

    private static OrderBy orderBy(String column, OrderBy.Direction descending) {
        return OrderBy
                .newBuilder()
                .setColumn(column)
                .setDirection(descending)
                .vBuild();
    }

    public static List<CollegeId> recordIds(Collection<EntityRecord> resultList) {
        return resultList
                .stream()
                .map(EntityRecord::getEntityId)
                .map(AnyPacker::unpack)
                .map(id -> (CollegeId) id)
                .collect(toList());
    }

    public static <T extends Comparable<T>> List<CollegeId>
    sortedIds(Collection<CollegeEntity> entities, Function<CollegeEntity, T> property) {
        return entities
                .stream()
                .sorted(comparing(property, nullsFirst(naturalOrder())))
                .map(AbstractEntity::id)
                .collect(toList());
    }

    public static <T extends Comparable<T>> List<T>
    sortedValues(Collection<CollegeEntity> entities, Function<CollegeEntity, T> property) {
        return entities
                .stream()
                .sorted(comparing(property, nullsFirst(naturalOrder())))
                .map(property)
                .collect(toList());
    }

    public static Pagination pagination(int pageSize) {
        return Pagination
                .newBuilder()
                .setPageSize(pageSize)
                .vBuild();
    }

    @CanIgnoreReturnValue
    public static List<CollegeEntity>
    createAndStoreEntities(RecordStorage<CollegeId> storage, int recordCount) {
        List<CollegeEntity> entities = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            CollegeEntity entity = createAndStoreEntity(storage);
            entities.add(entity);
        }
        return entities;
    }

    @CanIgnoreReturnValue
    public static List<CollegeEntity>
    createAndStoreEntitiesWithNullStudentCount(RecordStorage<CollegeId> storage, int recordCount) {
        List<CollegeEntity> entities = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            CollegeEntity entity = createAndStoreEntityWithNullStudentCount(storage);
            entities.add(entity);
        }
        return entities;
    }

    @CanIgnoreReturnValue
    public static List<CollegeEntity>
    createAndStoreEntities(RecordStorage<CollegeId> storage, Collection<String> names) {
        return names
                .stream()
                .map(name -> createAndStoreEntity(storage, name))
                .collect(toList());
    }

    @CanIgnoreReturnValue
    public static List<CollegeEntity>
    createAndStoreEntities(RecordStorage<CollegeId> storage, Collection<String> names,
                           int studentCount, boolean stateSponsored) {
        return names
                .stream()
                .map(name -> createAndStoreEntity(storage, name, studentCount, stateSponsored))
                .collect(toList());
    }

    @CanIgnoreReturnValue
    public static CollegeEntity createAndStoreEntity(RecordStorage<CollegeId> storage) {
        CollegeId id = newCollegeId();
        CollegeEntity entity = CollegeEntity.create(id, newCollege(id));
        storeEntity(storage, entity);
        return entity;
    }

    @CanIgnoreReturnValue
    private static CollegeEntity createAndStoreEntity(RecordStorage<CollegeId> storage,
                                                      String name,
                                                      int studentCount,
                                                      boolean stateSponsored) {
        CollegeId id = newCollegeId();
        College state = newCollege(id, name, studentCount, stateSponsored);
        CollegeEntity entity = CollegeEntity.create(id, state);
        storeEntity(storage, entity);
        return entity;
    }

    private static CollegeEntity
    createAndStoreEntityWithNullStudentCount(RecordStorage<CollegeId> storage) {
        CollegeId id = newCollegeId();
        College state = newCollege(id, 0);
        CollegeEntity entity = CollegeEntity.create(id, state);
        storeEntity(storage, entity);
        return entity;
    }

    private static CollegeEntity createAndStoreEntity(RecordStorage<CollegeId> storage,
                                                      String name) {
        CollegeId id = newCollegeId();
        College state = newCollege(id, name);
        CollegeEntity entity = CollegeEntity.create(id, state);
        storeEntity(storage, entity);
        return entity;
    }

    public static void storeEntity(RecordStorage<CollegeId> storage, CollegeEntity entity) {
        EntityRecord record = newEntityRecord(entity.id(), entity.state());
        EntityRecordWithColumns withColumns = create(record, entity, storage);
        storage.write(entity.id(), withColumns);
    }

    public static CollegeId newCollegeId() {
        return CollegeId
                .newBuilder()
                .setValue(newUuid())
                .vBuild();
    }

    private static College newCollege(CollegeId id) {
        return newCollege(id, id.getValue());
    }

    private static College newCollege(CollegeId id, String name) {
        return newCollege(id, name, randomStudentCount());
    }

    private static College newCollege(CollegeId id, int studentCount) {
        return newCollege(id, id.getValue(), studentCount);
    }

    private static College newCollege(CollegeId id, String name, int studentCount) {
        return College
                .newBuilder()
                .setId(id)
                .setName(name)
                .setAdmissionDeadline(randomTimestamp())
                .setPassingGrade(randomPassingGrade())
                .setStudentCount(studentCount)
                .setStateSponsored(RANDOM.nextBoolean())
                .vBuild();
    }

    private static College newCollege(CollegeId id, String name, int studentCount,
                                      boolean stateSponsored) {
        return College
                .newBuilder()
                .setId(id)
                .setName(name)
                .setAdmissionDeadline(randomTimestamp())
                .setPassingGrade(randomPassingGrade())
                .setStudentCount(studentCount)
                .setStateSponsored(stateSponsored)
                .vBuild();
    }

    private static int randomStudentCount() {
        return RANDOM.nextInt(1000);
    }

    private static double randomPassingGrade() {
        return RANDOM.nextDouble() * 9 + 1;
    }

    private static Timestamp randomTimestamp() {
        long seconds = RANDOM.nextInt(MAX_TIMESTAMP_SECONDS);
        return Timestamp
                .newBuilder()
                .setSeconds(seconds)
                .build();
    }

    public static FieldMask newFieldMask(String... paths) {
        return FieldMask
                .newBuilder()
                .addAllPaths(Arrays.asList(paths))
                .build();
    }

    public static void assertSortedBooleans(Iterable<Boolean> values) {
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

    public static List<Boolean> getStateSponsoredValues(Collection<EntityRecord> resultList) {
        return resultList
                .stream()
                .map(EntityRecord::getState)
                .map(state -> (College) unpack(state))
                .map(College::getStateSponsored)
                .collect(toList());
    }

    public static List<Integer> nullableStudentCount(Collection<EntityRecord> resultList) {
        return resultList
                .stream()
                .map(EntityRecord::getState)
                .map(state -> (College) unpack(state))
                .map(College::getStudentCount)
                .map(count -> count == 0 ? null : count)
                .collect(toList());
    }

    public static List<CollegeEntity> combine(Collection<CollegeEntity> nullEntities,
                                              Collection<CollegeEntity> regularEntities) {
        List<CollegeEntity> combination =
                concat(nullEntities.stream(), regularEntities.stream()).collect(toList());
        shuffle(combination);
        return unmodifiableList(combination);
    }

    public static EntityId newEntityId(Message message) {
        return EntityId
                .newBuilder()
                .setId(pack(message))
                .vBuild();
    }

    /*
     * Test Entity types
     ************************/

    public static class TestEntity extends RecordStorageTestEnv.TestCounterEntity {

        protected TestEntity(ProjectId id) {
            super(id);
        }
    }

    public static class EntityWithCustomColumnName extends AbstractEntity<ProjectId, Project> {

        public EntityWithCustomColumnName(ProjectId id) {
            super(id);
        }

        @Column(name = COLUMN_NAME_FOR_STORING)
        public int getValue() {
            return 0;
        }
    }

}
