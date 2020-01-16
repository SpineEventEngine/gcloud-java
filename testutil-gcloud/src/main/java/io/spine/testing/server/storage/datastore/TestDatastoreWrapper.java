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

package io.spine.testing.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.annotations.VisibleForTesting;
import io.spine.server.storage.datastore.DatastoreWrapper;
import io.spine.server.storage.datastore.Kind;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * Custom extension of the {@link DatastoreWrapper} for the integration testing.
 *
 * @see TestDatastoreStorageFactory
 */
public class TestDatastoreWrapper extends DatastoreWrapper {

    /**
     * Default time to wait before each read operation to ensure the data is consistent.
     *
     * <p>NOTE: enabled only if {@link #waitForConsistency} is {@code true}.
     */
    private static final int CONSISTENCY_AWAIT_TIME_MS = 10;
    private static final int CONSISTENCY_AWAIT_ITERATIONS = 20;

    /**
     * Due to eventual consistency, {@linkplain #dropTable(String) is performed iteratively until
     * the table has no records}.
     *
     * <p>This constant represents the maximum number of cleanup attempts before the execution
     * is continued.
     */
    private static final int MAX_CLEANUP_ATTEMPTS = 5;

    private static final Collection<String> kindsCache = new ArrayList<>();

    private final boolean waitForConsistency;

    protected TestDatastoreWrapper(Datastore datastore, boolean waitForConsistency) {
        super(datastore, NamespaceSupplier.singleTenant());
        this.waitForConsistency = waitForConsistency;
    }

    /**
     * Wraps a given Datastore.
     *
     * <p>The {@code waitForConsistency} parameter allows to add a delay to each write operation to
     * compensate for the eventual consistency of the storage.
     *
     * <p>The {@code waitForConsistency} parameter should usually be set to {@code false} when
     * working with a local Datastore emulator.
     */
    public static TestDatastoreWrapper wrap(Datastore datastore, boolean waitForConsistency) {
        checkNotNull(datastore);
        return new TestDatastoreWrapper(datastore, waitForConsistency);
    }

    @Override
    public KeyFactory keyFactory(Kind kind) {
        kindsCache.add(kind.value());
        return super.keyFactory(kind);
    }

    @Override
    public void createOrUpdate(Entity entity) {
        super.createOrUpdate(entity);
        waitForConsistency();
    }

    @Override
    public void create(Entity entity) throws DatastoreException {
        super.create(entity);
        waitForConsistency();
    }

    @Override
    public void update(Entity entity) throws DatastoreException {
        super.update(entity);
        waitForConsistency();
    }

    @Override
    protected void dropTable(String table) {
        if (!waitForConsistency) {
            super.dropTable(table);
        } else {
            dropTableConsistently(table);
        }
    }

    @SuppressWarnings("BusyWait")   // allows Datastore some time between cleanup attempts.
    private void dropTableConsistently(String table) {
        Integer remainingEntityCount = null;
        int cleanupAttempts = 0;

        while ((remainingEntityCount == null
                || remainingEntityCount > 0)
                && cleanupAttempts < MAX_CLEANUP_ATTEMPTS) {

            // sleep in between the cleanup attempts.
            if (cleanupAttempts > 0) {
                try {
                    Thread.sleep(CONSISTENCY_AWAIT_TIME_MS);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }

            StructuredQuery<Entity> query = Query.newEntityQueryBuilder()
                                                 .setKind(table)
                                                 .build();
            List<Entity> entities = newArrayList(read(query));
            remainingEntityCount = entities.size();

            if (remainingEntityCount > 0) {
                deleteEntities(entities);
                cleanupAttempts++;
            }
        }

        if (cleanupAttempts >= MAX_CLEANUP_ATTEMPTS) {
            throw newIllegalStateException(
                    "Cannot cleanup the table: %s. Remaining entity count is %d",
                    table, remainingEntityCount);
        }
    }

    private void waitForConsistency() {
        if (!waitForConsistency) {
            _debug().log("Wait for consistency is not required.");
            return;
        }
        _debug().log("Waiting for data consistency to establish.");

        doWaitForConsistency();
    }

    @SuppressWarnings("BusyWait")   // allow Datastore to become consistent before reading.
    @VisibleForTesting
    protected void doWaitForConsistency() {
        for (int awaitCycle = 0; awaitCycle < CONSISTENCY_AWAIT_ITERATIONS; awaitCycle++) {
            try {
                Thread.sleep(CONSISTENCY_AWAIT_TIME_MS);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Deletes all records from the datastore.
     */
    public void dropAllTables() {
        _debug().log("Dropping all tables...");
        for (String kind : kindsCache) {
            dropTable(kind);
        }

        kindsCache.clear();
    }
}
