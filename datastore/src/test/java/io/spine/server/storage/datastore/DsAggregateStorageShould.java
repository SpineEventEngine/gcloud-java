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

import com.google.cloud.datastore.EntityQuery;
import io.spine.client.TestActorRequestFactory;
import io.spine.core.CommandEnvelope;
import io.spine.server.BoundedContext;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateRepository;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageShould;
import io.spine.server.aggregate.given.AggregateRepositoryTestEnv.ProjectAggregate;
import io.spine.server.entity.Entity;
import io.spine.test.aggregate.ProjectId;
import io.spine.test.aggregate.command.AggAddTask;
import io.spine.testdata.Sample;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.spine.client.TestActorRequestFactory.newInstance;
import static io.spine.server.aggregate.given.Given.CommandMessage.addTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings("InstanceMethodNamingConvention")
public class DsAggregateStorageShould extends AggregateStorageShould {

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

    @BeforeClass
    public static void setUpClass() {
        datastoreFactory.setUp();
    }

    @After
    public void tearDownTest() {
        datastoreFactory.clear();
    }

    @AfterClass
    public static void tearDownClass() {
        datastoreFactory.tearDown();
    }

    @Override
    protected AggregateStorage<ProjectId> getStorage(Class<? extends Entity> cls) {
        return getStorage();
    }

    @Override
    protected <I> AggregateStorage<I> getStorage(Class<? extends I> idClass,
                                                 Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        return datastoreFactory.createAggregateStorage(aggregateClass);
    }

    @Test
    public void provide_access_to_DatastoreWrapper_for_extensibility() {
        final DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        final DatastoreWrapper datastore = storage.getDatastore();
        assertNotNull(datastore);
    }

    @Test
    public void provide_access_to_PropertyStorage_for_extensibility() {
        final DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        final DsPropertyStorage propertyStorage = storage.getPropertyStorage();
        assertNotNull(propertyStorage);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fail_to_write_invalid_record() {
        final DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        storage.writeRecord(Sample.messageOfType(ProjectId.class),
                            AggregateEventRecord.getDefaultInstance());
    }

    @Test
    public void set_limit_for_history_backward_query() {
        final DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        final int batchSize = 10;
        final AggregateReadRequest<ProjectId> request = new AggregateReadRequest<>(newId(),
                                                                                   batchSize);
        final EntityQuery historyBackwardQuery = storage.historyBackwardQuery(request);

        final int queryLimit = historyBackwardQuery.getLimit();
        assertEquals(batchSize, queryLimit);
    }

    @Test
    public void still_load_aggregates_properly_after_snapshot_trigger_decrease_at_runtime() {
        final ProjectAggregateRepository repository = new ProjectAggregateRepository();
        repository.initStorage(datastoreFactory);
        repository.setBoundedContext(BoundedContext.newBuilder()
                                                   .build());
        final ProjectId id = newId();
        final int initialSnapshotTrigger = 10;

        // To restore an aggregate using a snapshot and events.
        final int tasksCount = initialSnapshotTrigger * 2 - 1;

        repository.setSnapshotTrigger(initialSnapshotTrigger);
        final TestActorRequestFactory factory = newInstance(DsAggregateStorageShould.class);
        for (int i = 0; i < tasksCount; i++) {
            final AggAddTask command = addTask(id);
            final CommandEnvelope envelope = CommandEnvelope.of(factory.createCommand(command));
            repository.dispatch(envelope);
        }

        final int minimalSnapshotTrigger = 1;
        repository.setSnapshotTrigger(minimalSnapshotTrigger);
        final ProjectAggregate aggregate = repository.find(id)
                                                     .get();
        assertEquals(tasksCount, aggregate.getState()
                                          .getTaskCount());
    }

    private static class ProjectAggregateRepository
            extends AggregateRepository<ProjectId, ProjectAggregate> {

        @SuppressWarnings("RedundantMethodOverride") // To expose the method for testing.
        @Override
        protected void setSnapshotTrigger(int snapshotTrigger) {
            super.setSnapshotTrigger(snapshotTrigger);
        }
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(DsAggregateStorageShould.class);
    }
}
