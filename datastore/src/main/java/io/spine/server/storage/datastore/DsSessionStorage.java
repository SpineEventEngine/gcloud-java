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

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.WorkerId;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Optional;

import static com.google.cloud.Timestamp.fromProto;
import static com.google.cloud.datastore.Query.newEntityQueryBuilder;
import static io.spine.server.storage.datastore.DatastoreWrapper.MAX_ENTITIES_PER_WRITE_REQUEST;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * A Datastore-based storage which contains {@code ShardSessionRecord}s.
 */
public final class DsSessionStorage
        extends DsMessageStorage<ShardIndex, ShardSessionRecord, ShardSessionReadRequest> {

    private static final boolean multitenant = false;

    DsSessionStorage(DatastoreStorageFactory factory) {
        super(factory.systemWrapperFor(DsSessionStorage.class, multitenant), multitenant);
    }

    @Override
    protected ShardIndex idOf(ShardSessionRecord message) {
        return message.getIndex();
    }

    @Override
    protected MessageColumn<ShardSessionRecord>[] columns() {
        return Column.values();
    }

    /**
     * Obtains all the session records present in the storage.
     */
    Iterator<ShardSessionRecord> readAll() {
        return readAll(newEntityQueryBuilder(), MAX_ENTITIES_PER_WRITE_REQUEST);
    }

    /**
     * Obtains the session record for the shard with the given index.
     *
     * <p>The read operation is executed in a new transaction.
     */
    Optional<ShardSessionRecord> read(ShardIndex index) {
        return readTransactionally(new ShardSessionReadRequest(index));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Executes the write operation in a new transaction.
     */
    @Override
    public void write(ShardSessionRecord message) {
        writeTransactionally(message);
    }

    /**
     * Attempts to execute the update of the {@link ShardSessionRecord} in a scope of a new
     * Datastore transaction.
     *
     * <p>Returns the updated record if the update succeeded.
     *
     * <p>Returns {@code Optional.empty()} if the update could not be executed, because
     * the rules of the passed {@code RecordUpdate} prevented it.
     *
     * @param index
     *         index of a record to execute an update for
     * @param update
     *         an update to perform
     * @return a modified record, or {@code Optional.empty()} if the update could not be executed
     * @throws DatastoreException
     *         if there is a problem communicating with Datastore, or if the entity could not
     *         be updated due to a concurrent changes which have happened to the corresponding
     *         Datastore entity.
     */
    Optional<ShardSessionRecord> updateTransactionally(ShardIndex index, RecordUpdate update) {
        try (TransactionWrapper tx = newTransaction()) {
            Key key = key(index);
            Optional<Entity> result = tx.read(key);

            @Nullable ShardSessionRecord existing =
                    result.map(this::toMessage)
                          .orElse(null);
            Optional<ShardSessionRecord> updated = update.createOrUpdate(existing);
            if (updated.isPresent()) {
                ShardSessionRecord asRecord = updated.get();
                tx.createOrUpdate(toEntity(asRecord));
                tx.commit();
            }
            return updated;
        } catch (DatastoreException e) {
            throw e;
        } catch (RuntimeException e) {
            throw newIllegalStateException(
                    e, "Cannot update the `ShardSessionRecord` with index `%s` in a transaction.",
                    index);
        }
    }

    /**
     * A method object telling how to update {@link ShardSessionRecord}s depending on the current
     * state of the records in Datastore.
     */
    interface RecordUpdate {

        /**
         * Decides in which update the existing record should result.
         *
         * @param previous
         *         the previous record currently residing in the storage, or {@code null}
         *         if there is no such record
         * @return a version of the record to write to the storage,
         *         or {@code Optional.empty()} if no update should be performed
         */
        Optional<ShardSessionRecord> createOrUpdate(@Nullable ShardSessionRecord previous);
    }

    /**
     * The columns of the {@link ShardSessionRecord} message stored in Datastore.
     */
    private enum Column implements MessageColumn<ShardSessionRecord> {

        shard((m) -> {
            return LongValue.of(m.getIndex()
                                 .getIndex());
        }),

        total_shards((m) -> {
            return LongValue.of(m.getIndex()
                                 .getOfTotal());
        }),

        worker((m) -> {
            WorkerId worker = m.getWorker();
            String value = worker.getNodeId()
                                 .getValue() + '-' + worker.getValue();
            return StringValue.of(value);

        }),

        when_last_picked((m) -> {
            return TimestampValue.of(fromProto(m.getWhenLastPicked()));
        });

        /**
         * Obtains the value of the column from the given message.
         */
        private final Getter<ShardSessionRecord> getter;

        Column(Getter<ShardSessionRecord> getter) {
            this.getter = getter;
        }

        @Override
        public String columnName() {
            return name();
        }

        @Override
        public Getter<ShardSessionRecord> getter() {
            return getter;
        }
    }
}
