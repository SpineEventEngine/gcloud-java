/*
 * Copyright 2021, TeamDev. All rights reserved.
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
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.delivery.ShardSessionRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.base.Time.currentTime;

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
    public synchronized Optional<ShardProcessingSession> pickUp(ShardIndex index, NodeId nodeId) {
        checkNotNull(index);
        checkNotNull(nodeId);
        Optional<ShardSessionRecord> result =
                storage.updateTransactionally(index, new UpdateNodeIfAbsent(index, nodeId));
        return result.map(this::asSession);
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

    @Override
    protected ShardProcessingSession asSession(ShardSessionRecord record) {
        return new DsShardProcessingSession(record, () -> clearNode(record));
    }

    /**
     * Obtains the session storage which persists the session records.
     */
    protected DsSessionStorage storage() {
        return storage;
    }

    /**
     * Updates the {@code nodeId} for the {@link ShardSessionRecord} with the specified
     * {@link ShardIndex} if the record has not been picked by anyone.
     *
     * <p>If there is no such a record, creates a new record.
     */
    private static class UpdateNodeIfAbsent implements DsSessionStorage.RecordUpdate {

        private final ShardIndex index;
        private final NodeId nodeToSet;

        private UpdateNodeIfAbsent(ShardIndex index, NodeId set) {
            this.index = index;
            nodeToSet = set;
        }

        @Override
        public Optional<ShardSessionRecord> createOrUpdate(@Nullable ShardSessionRecord previous) {
            if (previous != null && previous.hasPickedBy()) {
                return Optional.empty();
            }
            ShardSessionRecord.Builder builder =
                    previous == null
                    ? ShardSessionRecord.newBuilder()
                                        .setIndex(index)
                    : previous.toBuilder();

            ShardSessionRecord updated =
                    builder.setPickedBy(nodeToSet)
                           .setWhenLastPicked(currentTime())
                           .vBuild();
            return Optional.of(updated);
        }
    }
}
