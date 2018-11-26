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

import com.google.cloud.datastore.EntityQuery;
import com.google.common.base.Suppliers;
import com.google.common.collect.Streams;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.core.CommandEnvelope;
import io.spine.server.BoundedContext;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateEventRecordVBuilder;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageTest;
import io.spine.server.aggregate.Snapshot;
import io.spine.server.aggregate.given.AggregateRepositoryTestEnv.ProjectAggregate;
import io.spine.server.entity.Entity;
import io.spine.server.storage.datastore.given.DsAggregateStorageTestEnv.NonProjectStateAggregate;
import io.spine.server.storage.datastore.given.aggregate.ProjectAggregateRepository;
import io.spine.test.aggregate.ProjectId;
import io.spine.test.aggregate.command.AggAddTask;
import io.spine.testdata.Sample;
import io.spine.testing.client.TestActorRequestFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Optional;

import static io.spine.server.aggregate.given.Given.CommandMessage.addTask;
import static io.spine.server.storage.datastore.TestDatastoreStorageFactory.defaultInstance;
import static io.spine.testing.client.TestActorRequestFactory.newInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("DsAggregateStorage should")
class DsAggregateStorageTest extends AggregateStorageTest {

    private static final TestDatastoreStorageFactory datastoreFactory = defaultInstance();

    @BeforeAll
    static void setUpClass() {
        datastoreFactory.setUp();
    }

    @AfterEach
    void tearDownTest() {
        datastoreFactory.clear();
    }

    @AfterAll
    static void tearDownClass() {
        datastoreFactory.tearDown();
    }

    @Override
    protected AggregateStorage<ProjectId> newStorage(Class<? extends Entity> cls) {
        @SuppressWarnings("unchecked") // Logically checked; OK for test purposes.
        Class<? extends Aggregate<ProjectId, ?, ?>> aggCls =
                (Class<? extends Aggregate<ProjectId, ?, ?>>) cls;
        return datastoreFactory.createAggregateStorage(aggCls);
    }

    @Override
    protected <I> AggregateStorage<I> newStorage(Class<? extends I> idClass,
                                                 Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        return datastoreFactory.createAggregateStorage(aggregateClass);
    }

    @SuppressWarnings("DuplicateStringLiteralInspection") // OK for tests.
    @Test
    @DisplayName("provide access to DatastoreWrapper for extensibility")
    void testAccessDatastoreWrapper() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        DatastoreWrapper datastore = storage.getDatastore();
        assertNotNull(datastore);
    }

    @SuppressWarnings("DuplicateStringLiteralInspection") // OK for tests.
    @Test
    @DisplayName("provide access to PropertyStorage for extensibility")
    void testAccessPropertyStorage() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        DsPropertyStorage propertyStorage = storage.getPropertyStorage();
        assertNotNull(propertyStorage);
    }

    @Test
    @DisplayName("fail to write invalid record")
    void testFailOnInvalidRecord() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        assertThrows(IllegalArgumentException.class,
                     () -> storage.writeRecord(Sample.messageOfType(ProjectId.class),
                                               AggregateEventRecord.getDefaultInstance()));
    }

    @Test
    @DisplayName("not set limit for history backward query")
    void testHistoryQueryLimit() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        int batchSize = 10;
        AggregateReadRequest<ProjectId> request = new AggregateReadRequest<>(newId(),
                                                                             batchSize);
        EntityQuery historyBackwardQuery = storage.historyBackwardQuery(request);

        Integer queryLimit = historyBackwardQuery.getLimit();
        assertNull(queryLimit);
    }

    @Test
    @DisplayName("read the event count stored in the old format")
    void readEventCountOldFormat() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        ProjectId id = newId();
        RecordId oldFormatId = storage.toRecordId(id);
        int eventCount = 15;
        storage.getPropertyStorage()
               .write(oldFormatId, Int32Value.newBuilder()
                                             .setValue(eventCount)
                                             .build());
        int actualEventCount = storage.readEventCountAfterLastSnapshot(id);
        assertEquals(eventCount, actualEventCount);
    }

    @Test
    @DisplayName("not overwrite the event count when saving other aggregate type")
    void notOverwriteEventCount() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        DsAggregateStorage<ProjectId> secondStorage = (DsAggregateStorage<ProjectId>)
                newStorage(ProjectId.class, NonProjectStateAggregate.class);

        ProjectId id = newId();
        int firstCount = 15;
        storage.writeEventCountAfterLastSnapshot(id, firstCount);
        int secondCount = 17;
        secondStorage.writeEventCountAfterLastSnapshot(id, secondCount);

        int actualFirstCount = storage.readEventCountAfterLastSnapshot(id);
        int actualSecondCount = secondStorage.readEventCountAfterLastSnapshot(id);
        assertEquals(firstCount, actualFirstCount);
        assertEquals(secondCount, actualSecondCount);
    }

    @Test
    @DisplayName("not overwrite the snapshot when saving a new one")
    void notOverwriteSnapshot() throws InterruptedException {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        ProjectId id = newId();

        writeSnapshotWithTimestamp(id, Timestamps.fromMillis(15));
        writeSnapshotWithTimestamp(id, Timestamps.fromMillis(15000));

        int batchSize = 10;
        AggregateReadRequest<ProjectId> request = new AggregateReadRequest<>(id, batchSize);
        Iterator<AggregateEventRecord> records = storage.historyBackward(request);
        long snapshotCount = Streams.stream(records)
                                    .count();
        assertEquals(2, snapshotCount);
    }

    @Nested
    class DynamicSnapshotTrigger {

        private ProjectAggregateRepository repository;
        private TestActorRequestFactory factory;
        private ProjectId id;

        @BeforeEach
        void setUp() {
            BoundedContext boundedContext =
                    BoundedContext.newBuilder()
                                  .setName(DsAggregateStorageTest.class.getName())
                                  .setStorageFactorySupplier(Suppliers.ofInstance(datastoreFactory))
                                  .build();
            repository = new ProjectAggregateRepository();
            boundedContext.register(repository);

            factory = newInstance(DsAggregateStorageTest.class);
            id = newId();
        }

        @Test
        @DisplayName("still load aggregates properly after snapshot trigger decrease at runtime")
        void testLoadHistoryAfterSnapshotTriggerChange() {
            int initialSnapshotTrigger = 10;

            // To restore an aggregate using a snapshot and events.
            int tasksCount = initialSnapshotTrigger * 2 - 1;

            repository.setSnapshotTrigger(initialSnapshotTrigger);
            for (int i = 0; i < tasksCount; i++) {
                AggAddTask command = addTask(id);
                CommandEnvelope envelope = CommandEnvelope.of(factory.createCommand(command));
                ProjectId target = repository.dispatch(envelope);
                assertEquals(id, target);
            }

            int minimalSnapshotTrigger = 1;
            repository.setSnapshotTrigger(minimalSnapshotTrigger);
            Optional<ProjectAggregate> optional = repository.find(id);
            assertTrue(optional.isPresent());
            ProjectAggregate aggregate = optional.get();
            assertEquals(tasksCount, aggregate.getState()
                                              .getTaskCount());
        }
    }

    private void writeSnapshotWithTimestamp(ProjectId id, Timestamp timestamp) {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        Snapshot snapshot = Snapshot.newBuilder()
                                    .setTimestamp(timestamp)
                                    .build();
        AggregateEventRecord record = AggregateEventRecordVBuilder
                .newBuilder()
                .setSnapshot(snapshot)
                .build();
        storage.writeRecord(id, record);
    }
}
