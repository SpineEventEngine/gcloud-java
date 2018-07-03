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
import io.spine.client.TestActorRequestFactory;
import io.spine.core.CommandEnvelope;
import io.spine.server.BoundedContext;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageTest;
import io.spine.server.aggregate.given.AggregateRepositoryTestEnv.ProjectAggregate;
import io.spine.server.entity.Entity;
import io.spine.server.storage.datastore.given.DsAggregateStorageTestEnv.ProjectAggregateRepository;
import io.spine.test.aggregate.ProjectId;
import io.spine.test.aggregate.command.AggAddTask;
import io.spine.testdata.Sample;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.spine.client.TestActorRequestFactory.newInstance;
import static io.spine.server.aggregate.given.Given.CommandMessage.addTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("DsAggregateStorage should")
class DsAggregateStorageTest extends AggregateStorageTest {

    private static final TestDatastoreStorageFactory datastoreFactory;

    // Guarantees any stacktrace to be informative
    static {
        try {
            datastoreFactory = TestDatastoreStorageFactory.getDefaultInstance();
        } catch (Throwable e) {
            log().error("Failed to initialize local datastore factory", e);
            throw new RuntimeException(e);
        }
    }

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
    @DisplayName("set limit for history backward query")
    void testHistoryQueryLimit() {
        DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        int batchSize = 10;
        AggregateReadRequest<ProjectId> request = new AggregateReadRequest<>(newId(),
                                                                             batchSize);
        EntityQuery historyBackwardQuery = storage.historyBackwardQuery(request);

        int queryLimit = historyBackwardQuery.getLimit();
        assertEquals(batchSize, queryLimit);
    }

    @Test
    @DisplayName("still load aggregates properly after snapshot trigger decrease at runtime")
    void testLoadHistoryAfterSnapshotTriggerChange() {
        BoundedContext boundedContext =
                BoundedContext.newBuilder()
                              .setName(DsAggregateStorageTest.class.getName())
                              .setStorageFactorySupplier(Suppliers.ofInstance(datastoreFactory))
                              .build();
        ProjectAggregateRepository repository = new ProjectAggregateRepository();
        boundedContext.register(repository);

        ProjectId id = newId();
        int initialSnapshotTrigger = 10;

        // To restore an aggregate using a snapshot and events.
        int tasksCount = initialSnapshotTrigger * 2 - 1;

        repository.setSnapshotTrigger(initialSnapshotTrigger);
        TestActorRequestFactory factory = newInstance(DsAggregateStorageTest.class);
        for (int i = 0; i < tasksCount; i++) {
            AggAddTask command = addTask(id);
            CommandEnvelope envelope = CommandEnvelope.of(factory.createCommand(command));
            repository.dispatch(envelope);
        }

        int minimalSnapshotTrigger = 1;
        repository.setSnapshotTrigger(minimalSnapshotTrigger);
        ProjectAggregate aggregate = repository.find(id).get();
        assertEquals(tasksCount, aggregate.getState()
                                          .getTaskCount());
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(DsAggregateStorageTest.class);
    }
}
