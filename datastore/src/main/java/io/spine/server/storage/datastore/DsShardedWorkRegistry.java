/*
 * Copyright 2022, TeamDev. All rights reserved.
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

import com.google.protobuf.Duration;
import io.spine.logging.Logging;
import io.spine.server.NodeId;
import io.spine.server.delivery.AbstractWorkRegistry;
import io.spine.server.delivery.PickUpOutcome;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.WorkerId;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.base.Time.currentTime;
import static io.spine.server.delivery.PickUpOutcomeMixin.alreadyPicked;
import static io.spine.server.delivery.PickUpOutcomeMixin.pickedUp;

/**
 * A {@link io.spine.server.delivery.ShardedWorkRegistry} based on the Google Datastore storage.
 *
 * <p>The sharded work is stored as {@link ShardSessionRecord}s in the Datastore. Each time
 * the session is {@linkplain #pickUp(ShardIndex, NodeId) picked up}, the corresponding
 * {@code Entity} in the Datastore is updated.
 *
 * <p>It is recommended to use this implementation with Cloud Firestore in Datastore mode,
 * as it provides the strong consistency for queries.
 */
public class DsShardedWorkRegistry extends AbstractWorkRegistry implements Logging {

    private final DsSessionStorage storage;

    /**
     * Creates an instance of registry using the {@link DatastoreStorageFactory} passed.
     *
     * <p>The storage initialized by this registry is always single-tenant, since its
     * records represent the application-wide registry of server nodes, which aren't split
     * by tenant.
     */
    public DsShardedWorkRegistry(DatastoreStorageFactory factory) {
        super();
        checkNotNull(factory);
        this.storage = new DsSessionStorage(factory);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The potential concurrent access to the same record is handled by using the Datastore
     * transaction mechanism. In case of any parallel executions of {@code pickUp} operation,
     * the one started earlier wins.
     */
    @Override
    public synchronized PickUpOutcome pickUp(ShardIndex index, NodeId nodeId) {
        checkNotNull(index);
        checkNotNull(nodeId);

        WorkerId worker = currentWorkerFor(nodeId);
        UpdateWorkerIfAbsent updateAction = new UpdateWorkerIfAbsent(index, worker);
        Optional<ShardSessionRecord> result = storage.updateTransactionally(index, updateAction);
        if (result.isPresent()) {
            return pickedUp(result.get());
        } else {
            ShardSessionRecord notUpdated = updateAction.previous().get();
            return alreadyPicked(notUpdated.getWorker(), notUpdated.getWhenLastPicked());
        }
    }

    @Override
    public void release(ShardSessionRecord session) {
        clearNode(session);
    }

    /**
     * Creates a worker ID by combining the given node ID with the ID of the current Java thread,
     * in which the execution in performed.
     */
    @Override
    protected WorkerId currentWorkerFor(NodeId id) {
        long threadId = Thread.currentThread().getId();
        return WorkerId.newBuilder()
                .setNodeId(id)
                .setValue(Long.toString(threadId))
                .vBuild();
    }

    @Override
    public synchronized Iterable<ShardIndex> releaseExpiredSessions(Duration inactivityPeriod) {
        return super.releaseExpiredSessions(inactivityPeriod);
    }

    @Override
    protected synchronized void clearNode(ShardSessionRecord session) {
        super.clearNode(session);
    }

    @Override
    protected Iterator<ShardSessionRecord> allRecords() {
        return storage().readAll();
    }

    @Override
    protected void write(ShardSessionRecord session) {
        storage().writeTransactionally(session);
    }

    @Override
    protected Optional<ShardSessionRecord> find(ShardIndex index) {
        Optional<ShardSessionRecord> read = storage().read(index);
        return read;
    }

    /**
     * Obtains the session storage which persists the session records.
     */
    protected DsSessionStorage storage() {
        return storage;
    }

    /**
     * Updates the {@code workerId} for the {@link ShardSessionRecord} with the specified
     * {@link ShardIndex} if the record has not been picked by anyone.
     *
     * <p>If there is no such a record, creates a new record.
     *
     * <p>Preserves the record state before updating if the supplied record is not {@code null}.
     */
    private static class UpdateWorkerIfAbsent implements DsSessionStorage.RecordUpdate {

        private final ShardIndex index;
        private final WorkerId workerToSet;
        private ShardSessionRecord previous;

        private UpdateWorkerIfAbsent(ShardIndex index, WorkerId worker) {
            this.index = index;
            workerToSet = worker;
        }

        @Override
        public Optional<ShardSessionRecord> createOrUpdate(@Nullable ShardSessionRecord previous) {
            if (previous != null) {
                this.previous = previous;
                if (previous.hasWorker()) {
                    return Optional.empty();
                }
            }
            ShardSessionRecord.Builder builder =
                    previous == null
                    ? ShardSessionRecord.newBuilder()
                                        .setIndex(index)
                    : previous.toBuilder();

            ShardSessionRecord updated =
                    builder.setWorker(workerToSet)
                           .setWhenLastPicked(currentTime())
                           .vBuild();
            return Optional.of(updated);
        }

        /**
         * Returns the {@code ShardSessionRecord} state before the update is executed, or empty
         * {@code Optional} if the is no previous record or
         * the {@linkplain #createOrUpdate(ShardSessionRecord) createOrUpdate()} is not called yet.
         */
        private Optional<ShardSessionRecord> previous() {
            return Optional.ofNullable(previous);
        }
    }
}
