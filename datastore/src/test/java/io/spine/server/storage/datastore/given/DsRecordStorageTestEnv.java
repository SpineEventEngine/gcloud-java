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
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;

import java.util.List;

import static com.google.common.collect.Lists.asList;
import static io.spine.base.Time.getCurrentTime;
import static io.spine.client.OrderBy.Direction.ASCENDING;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.server.entity.storage.EntityRecordWithColumns.create;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * A test environment for {@link io.spine.server.storage.datastore.DsRecordStorageTest}
 *
 * @author Mykhailo Drachuk
 */
public class DsRecordStorageTestEnv {

    public static final String COLUMN_NAME_FOR_STORING = "columnName";

    /**
     * Prevents instantiation of this test environment.
     */
    private DsRecordStorageTestEnv() {
    }

    public static TestDatastoreStorageFactory datastoreFactory() {
        return TestDatastoreStorageFactory.getDefaultInstance();
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

    public static EntityId newEntityId(TestConstCounterEntity targetEntity) {
        return EntityId.newBuilder()
                       .setId(pack(targetEntity.getId()))
                       .build();
    }

    public static List<EntityId> newEntityIds(List<TestConstCounterEntity> targetEntities) {
        return targetEntities.stream()
                             .map(entity -> EntityId.newBuilder()
                                                    .setId(pack(entity.getId()))
                                                    .build())
                             .collect(toList());
    }

    public static void storeEntity(RecordStorage<ProjectId> storage,
                                   TestConstCounterEntity entity) {
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
        return OrderByVBuilder.newBuilder()
                              .setColumn(column)
                              .setDirection(ASCENDING)
                              .build();
    }

    public static List<ProjectId> recordIds(List<EntityRecord> resultList) {
        return resultList.stream()
                         .map(EntityRecord::getEntityId)
                         .map(AnyPacker::unpack)
                         .map(id -> (ProjectId) id)
                         .collect(toList());
    }

    public static List<ProjectId> idsSortedByName(List<TestConstCounterEntity> entities) {
        return entities.stream()
                       .sorted(comparing(TestConstCounterEntity::getCounterName))
                       .map(AbstractEntity::getId)
                       .collect(toList());
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
}
