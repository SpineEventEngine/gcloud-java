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

package io.spine.server.storage.datastore;

import com.google.protobuf.Duration;
import io.spine.logging.Logging;
import io.spine.server.NodeId;
import io.spine.server.delivery.AbstractWorkRegistry;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.delivery.ShardSessionRecord;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

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
public class DsShardedWorkRegistry
        extends AbstractWorkRegistry
        implements Logging {

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
     * <p>When picking up a shard, performs a double check to ensure that the write into database
     * was not overridden by another node. If the write was overridden, gives up the shard to
     * the other node and returns {@code Optional.empty()}.
     */
    @Override
    public synchronized Optional<ShardProcessingSession> pickUp(ShardIndex index, NodeId nodeId) {
        Optional<ShardProcessingSession> picked = super.pickUp(index, nodeId);
        return picked.filter(session -> pickedBy(index, nodeId));
    }

    private boolean pickedBy(ShardIndex index, NodeId nodeId) {
        Optional<ShardSessionRecord> stored = find(index);
        return stored.map(record -> record.getPickedBy().equals(nodeId))
                     .orElse(false);
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
        storage().write(session);
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

    protected DsSessionStorage storage() {
        return storage;
    }
}
