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

package io.spine.server.storage.datastore.delivery;

import com.google.protobuf.Duration;
import io.spine.logging.Logging;
import io.spine.server.ContextSpec;
import io.spine.server.NodeId;
import io.spine.server.delivery.AbstractWorkRegistry;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.WorkerId;
import io.spine.server.storage.datastore.DatastoreStorageFactory;

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
 * <p>This storage uses transactions for read and write operations. It is also recommended using
 * this implementation with Cloud Firestore in Datastore mode, as it enforces serializable isolation
 * for transactions.
 */
public class DsShardedWorkRegistry extends AbstractWorkRegistry implements Logging {

    private final DsSessionStorage storage;

    /**
     * Creates an instance of registry using the {@link DatastoreStorageFactory} passed.
     *
     * <p>The storage initialized by this registry serves the records representing
     * the application-wide registry of server nodes and their respective shard deliveries.
     * Therefore, it is recommended to create this registry in scope of a single-tenant
     * system-internal Bounded Context.
     *
     * @param factory
     *         factory to create a record storage for the registry
     * @param context
     *         specification of the Bounded Context in which the created storage will reside
     */
    public DsShardedWorkRegistry(DatastoreStorageFactory factory, ContextSpec context) {
        super();
        checkNotNull(factory);
        this.storage = new DsSessionStorage(factory, context);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The potential concurrent access to the same record is handled by using the Datastore
     * transaction mechanism. In case of any parallel executions of {@code pickUp} operation,
     * the one started earlier wins.
     */
    @Override
    public synchronized Optional<ShardProcessingSession> pickUp(ShardIndex index, NodeId node) {
        checkNotNull(index);
        checkNotNull(node);
        var worker = currentWorkerFor(node);
        var operation = new SetWorkerIfAbsent(index, worker);
        var record = storage().updateTransactionally(index, operation);
        var session = record.map(this::asSession);
        return session;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation uses an identifier of the current thread as a {@code WorkerId}.
     */
    @Override
    protected WorkerId currentWorkerFor(NodeId node) {
        var currentThread = Thread.currentThread().getId();
        var worker = WorkerId
                .newBuilder()
                .setNodeId(node)
                .setValue(String.valueOf(currentThread))
                .vBuild();
        return worker;
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
        var read = storage().read(index);
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
}
