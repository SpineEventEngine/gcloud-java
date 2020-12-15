/*
 * Copyright 2020, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.collect.Streams;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.base.Tests;
import io.spine.core.Event;
import io.spine.core.Version;
import io.spine.protobuf.AnyPacker;
import io.spine.server.BoundedContext;
import io.spine.server.ContextSpec;
import io.spine.server.ServerEnvironment;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateHistory;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageTest;
import io.spine.server.aggregate.Snapshot;
import io.spine.server.aggregate.given.repo.ProjectAggregate;
import io.spine.server.entity.Entity;
import io.spine.server.storage.datastore.given.CountingDatastoreWrapper;
import io.spine.server.storage.datastore.given.aggregate.ProjectAggregateRepository;
import io.spine.server.type.CommandEnvelope;
import io.spine.test.aggregate.Project;
import io.spine.test.aggregate.ProjectId;
import io.spine.test.aggregate.command.AggAddTask;
import io.spine.test.storage.StateImported;
import io.spine.testdata.Sample;
import io.spine.testing.client.TestActorRequestFactory;
import io.spine.testing.server.TestEventFactory;
import io.spine.testing.server.storage.datastore.SpyStorageFactory;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Time.currentTime;
import static io.spine.core.Versions.increment;
import static io.spine.core.Versions.zero;
import static io.spine.server.ContextSpec.singleTenant;
import static io.spine.server.aggregate.given.Given.CommandMessage.addTask;
import static io.spine.server.storage.datastore.DatastoreWrapper.MAX_ENTITIES_PER_WRITE_REQUEST;
import static io.spine.server.storage.datastore.given.DsRecordStorageTestEnv.datastoreFactory;
import static io.spine.server.storage.datastore.given.TestEnvironment.singleTenantSpec;
import static io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory.local;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("`DsAggregateStorage` should")
class DsAggregateStorageTest extends AggregateStorageTest {

    private static final TestDatastoreStorageFactory datastoreFactory = local();

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
    protected AggregateStorage<ProjectId> newStorage(Class<? extends Entity<?, ?>> cls) {
        @SuppressWarnings("unchecked") // Logically checked; OK for test purposes.
                Class<? extends Aggregate<ProjectId, ?, ?>> aggCls =
                (Class<? extends Aggregate<ProjectId, ?, ?>>) cls;
        return datastoreFactory.createAggregateStorage(singleTenantSpec(), aggCls);
    }

    @Override
    protected <I> AggregateStorage<I> newStorage(Class<? extends I> idClass,
                                                 Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        return datastoreFactory.createAggregateStorage(singleTenantSpec(), aggregateClass);
    }

    @SuppressWarnings("DuplicateStringLiteralInspection") // OK for tests.
    @Test
    @DisplayName("provide access to `DatastoreWrapper` for extensibility")
    void testAccessDatastoreWrapper() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) storage();
        DatastoreWrapper datastore = storage.datastore();
        assertNotNull(datastore);
    }

    @Test
    @DisplayName("fail to write invalid record")
    void testFailOnInvalidRecord() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) storage();
        assertThrows(IllegalArgumentException.class,
                     () -> storage.writeRecord(Sample.messageOfType(ProjectId.class),
                                               AggregateEventRecord.getDefaultInstance()));
    }

    @Test
    @DisplayName("not set limit for history backward query")
    void testHistoryQueryLimit() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) storage();
        int batchSize = 10;
        AggregateReadRequest<ProjectId> request = new AggregateReadRequest<>(newId(),
                                                                             batchSize);
        EntityQuery historyBackwardQuery = storage.historyBackwardQuery(request);

        Integer queryLimit = historyBackwardQuery.getLimit();
        assertNull(queryLimit);
    }

    @Test
    @DisplayName("not overwrite the snapshot when saving a new one")
    void notOverwriteSnapshot() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) storage();
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
    @DisplayName("truncate efficiently")
    class TruncateEfficiently {

        private final ContextSpec contextSpec = singleTenant(TruncateEfficiently.class.getName());
        private DsAggregateStorage<ProjectId> storage;
        private CountingDatastoreWrapper datastoreWrapper;

        @BeforeEach
        void setUp() {
            datastoreWrapper = new CountingDatastoreWrapper(datastoreFactory().datastore(), false);
            SpyStorageFactory.injectWrapper(datastoreWrapper);
            SpyStorageFactory storageFactory = new SpyStorageFactory();
            storage = (DsAggregateStorage<ProjectId>)
                    storageFactory.createAggregateStorage(contextSpec, ProjectAggregate.class);
        }

        @Test
        @DisplayName("when having bulk of records stored")
        void withBulkOfRecords() {
            ProjectId id = newId();
            AggregateHistory.Builder history = AggregateHistory.newBuilder();
            Version version = zero();
            Any state = AnyPacker.pack(Project.getDefaultInstance());

            // Store *max entities per delete request* + 1 events.
            TestEventFactory factory = TestEventFactory.newInstance(DsAggregateStorageTest.class);
            int eventCount = MAX_ENTITIES_PER_WRITE_REQUEST + 1;
            StateImported eventMessage = StateImported
                    .newBuilder()
                    .setState(state)
                    .vBuild();
            for (int i = 0; i < eventCount; i++) {
                version = increment(version);
                Event event = factory.createEvent(eventMessage, version, currentTime());
                history.addEvent(event);
            }
            storage.write(id, history.build());

            // Store the snapshot after events.
            version = increment(version);
            Snapshot latestSnapshot = Snapshot
                    .newBuilder()
                    .setState(state)
                    .setVersion(version)
                    .setTimestamp(currentTime())
                    .vBuild();
            Event latestEvent = factory.createEvent(eventMessage,
                                                    increment(version),
                                                    currentTime());
            AggregateHistory historyAfterSnapshot = AggregateHistory
                    .newBuilder()
                    .setSnapshot(latestSnapshot)
                    .addEvent(latestEvent)
                    .vBuild();
            storage.write(id, historyAfterSnapshot);

            // Order removal of all records before the snapshot.
            storage.truncateOlderThan(0);

            // Check that the number of operations is minimum possible.
            int expectedOperationCount = eventCount / MAX_ENTITIES_PER_WRITE_REQUEST + 1;
            assertThat(datastoreWrapper.deleteCount())
                    .isEqualTo(expectedOperationCount);
        }
    }

    @Nested
    class DynamicSnapshotTrigger {

        private ProjectAggregateRepository repository;
        private TestActorRequestFactory factory;
        private ProjectId id;

        @BeforeEach
        void setUp() {
            ServerEnvironment.when(Tests.class)
                             .use(datastoreFactory);
            repository = new ProjectAggregateRepository();
            BoundedContext.singleTenant(DsAggregateStorageTest.class.getName())
                          .add(repository)
                          .build();

            factory = new TestActorRequestFactory(DsAggregateStorageTest.class);
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
                repository.dispatch(envelope);
            }

            int minimalSnapshotTrigger = 1;
            repository.setSnapshotTrigger(minimalSnapshotTrigger);
            Optional<ProjectAggregate> optional = repository.find(id);
            assertTrue(optional.isPresent());
            ProjectAggregate aggregate = optional.get();
            assertEquals(tasksCount, aggregate.state()
                                              .getTaskCount());
        }
    }

    private void writeSnapshotWithTimestamp(ProjectId id, Timestamp timestamp) {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) storage();
        Snapshot snapshot = Snapshot
                .newBuilder()
                .setTimestamp(timestamp)
                .vBuild();
        AggregateEventRecord record = AggregateEventRecord
                .newBuilder()
                .setSnapshot(snapshot)
                .vBuild();
        storage.writeRecord(id, record);
    }
}
