/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.rpc.Code;
import io.spine.server.ContextSpec;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.storage.MessageRecordSpec;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.datastore.DatastoreStorageFactory;
import io.spine.server.storage.datastore.config.StorageConfiguration;
import io.spine.server.storage.datastore.config.TxSetting;
import io.spine.server.storage.datastore.record.DsEntitySpec;
import io.spine.server.storage.datastore.record.DsRecordStorage;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.datastore.delivery.DsSessionStorage.UpdateResult.remainedAsIs;
import static io.spine.server.storage.datastore.delivery.DsSessionStorage.UpdateResult.updatedSuccessfully;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * A Datastore-based storage which contains {@code ShardSessionRecord}s.
 *
 * <p>This storage serves the records on shard delivery sessions. As long as
 * {@link io.spine.server.delivery.Delivery Delivery} is a system routine that typically serves
 * several Domain Bounded Contexts, it is recommended to make this storage a single-tenant one.
 *
 * <p>As all storages require a definition of the Bounded Context to which they belong,
 * library users are required to provide such for this storage as well. In most cases, this
 * will be a system-internal Bounded Context describing the Delivery.
 */
public final class DsSessionStorage
        extends DsRecordStorage<ShardIndex, ShardSessionRecord> {

    private static final boolean multitenant = false;
    private final MessageRecordSpec<ShardIndex, ShardSessionRecord> spec;

    /**
     * Creates a new instance of this storage.
     *
     * <p>Unlike other storages, this one typically resides in a system-internal Bounded Context
     * which describes Delivery and other system routines. Library users are required to supply
     * the reference to such a context, as it is most likely initialized prior to creating
     * this storage.
     *
     * @param factory
     *         the storage factory on top of which this storage is to be created
     * @param context
     *         the Bounded Context in scope of which this storage is created
     */
    @SuppressWarnings("WeakerAccess")   /* This ctor is a part of public API. */
    public DsSessionStorage(DatastoreStorageFactory factory, ContextSpec context) {
        super(configureWith(factory, context));
        this.spec = messageSpec();
    }

    private static StorageConfiguration<ShardIndex, ShardSessionRecord>
    configureWith(DatastoreStorageFactory factory, ContextSpec context) {
        var wrapper = factory.systemWrapperFor(DsSessionStorage.class, multitenant);
        var config = StorageConfiguration.<ShardIndex, ShardSessionRecord>newBuilder()
                .withDatastore(wrapper)
                .withRecordSpec(newRecordSpec())
                .withContext(context)
                .withMapping(factory.columnMapping())
                .withTxSetting(TxSetting.enabled())
                .build();
        return config;
    }

    private static DsEntitySpec<ShardIndex, ShardSessionRecord> newRecordSpec() {
        var spec = messageSpec();
        var result = new DsEntitySpec<>(spec);
        return result;
    }

    private static MessageRecordSpec<ShardIndex, ShardSessionRecord> messageSpec() {
        @SuppressWarnings("ConstantConditions")     /* Protobuf getters never return `nulls`. */
                var spec = new MessageRecordSpec<>(
                ShardIndex.class,
                ShardSessionRecord.class,
                ShardSessionRecord::getIndex,
                SessionRecordColumn.definitions()
        );
        return spec;
    }

    /**
     * Obtains the session record for the shard with the given index.
     *
     * <p>The read operation is executed in a new transaction.
     */
    @Override
    public Optional<ShardSessionRecord> read(ShardIndex index) {
        var key = keyOf(index);
        try (var tx = newTransaction()) {
            var result = tx.read(key);
            tx.commit();
            return result.map(this::toRecord);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides the method in order to expose it to this package.
     */
    @Override
    protected Iterator<ShardSessionRecord> readAll() {
        return super.readAll();
    }

    /**
     * Writes the record to the storage in a new transaction.
     */
    @SuppressWarnings("OverlyBroadCatchBlock")  /* Treating all exceptions similarly. */
    public void write(ShardSessionRecord message) {
        try (var tx = newTransaction()) {
            var record = appendColumns(message);
            var entity = entityRecordToEntity(record);
            tx.createOrUpdate(entity);
            tx.commit();
        } catch (RuntimeException e) {
            throw newIllegalStateException(
                    e, "Cannot write a `ShardSessionRecord` in a transaction.");
        }
    }

    /**
     * Attempts to execute the update of the {@link ShardSessionRecord} in a scope of a new
     * Datastore transaction.
     *
     * <p>Returns the updated record if the update succeeded.
     *
     * <p>Returns {@code Optional.empty()} if the record was concurrently modified
     * by another node during the lifespan of this transaction. Another reason
     * for returning {@code Optional.empty()} may be the implementation
     * of passed {@code PrepareForWrite}, which prevented the update from happening.
     *
     * @param index
     *         index of a record to execute an update for
     * @param update
     *         an update to perform
     * @return a modified record, or {@code Optional.empty()} if the update could not be executed
     * @throws DatastoreException
     *         if there is a technical issue communicating with Datastore;
     *         please note this does NOT include the failures related
     *         to the transaction failure, as they are mean the record was not updated
     *         due to concurrent modification; in this case {@code Optional.empty()} is returned
     */
    UpdateResult
    updateTransactionally(ShardIndex index, PrepareForWrite update) throws DatastoreException {
        try (var tx = newTransaction()) {
            var key = keyOf(index);
            var result = tx.read(key);

            @Nullable ShardSessionRecord existing =
                    result.map(this::toRecord)
                          .orElse(null);
            var toWrite = update.prepare(existing);
            if (toWrite.isPresent()) {
                var asRecord = toWrite.get();
                tx.createOrUpdate(toEntity(asRecord));
                tx.commit();
                return updatedSuccessfully(asRecord);
            }
            if (existing != null){
                return remainedAsIs(existing);
            }
        } catch (DatastoreException e) {
            var errorCode = e.getCode();

            // We need to distinguish the transaction-related failures from other reasons.
            // Therefore, we treat `ABORTED` as such, which prevented the transactional update
            // meaning someone else had modified the record.
            //
            // In all other cases, the original exception most likely signalizes
            // of technical issues. Therefore, it is rethrown as-is.
            //
            // See the original documentation on RPC return codes for more detail.
            if (Code.ABORTED.getNumber() == errorCode) {
                var existing = read(index);
                if(existing.isPresent()) {
                    return remainedAsIs(existing.get());
                }
            }
            throw e;
        } catch (RuntimeException e) {
            throw newIllegalStateException(
                    e, "Cannot update the `ShardSessionRecord` with index `%s` in a transaction.",
                    index);
        }
        throw newIllegalStateException(
                "Cannot update the `ShardSessionRecord` with index `%s` in a transaction. " +
                        "There seem to be neither existing record, nor updated one in the storage. " +
                        "Please perform some manual investigation.",
                index);
    }

    private Entity toEntity(ShardSessionRecord record) {
        var withCols = appendColumns(record);
        var result = entityRecordToEntity(withCols);
        return result;
    }

    private RecordWithColumns<ShardIndex, ShardSessionRecord> appendColumns(ShardSessionRecord r) {
        var withCols = RecordWithColumns.create(r, spec);
        return withCols;
    }

    static final class UpdateResult {
        private final @Nullable ShardSessionRecord success;
        private final @Nullable ShardSessionRecord actual;

        private UpdateResult(@Nullable ShardSessionRecord success,
                            @Nullable ShardSessionRecord actual) {
            this.success = success;
            this.actual = actual;
        }

        static UpdateResult updatedSuccessfully(ShardSessionRecord success) {
            checkNotNull(success);
            return new UpdateResult(success, null);
        }

        static UpdateResult remainedAsIs(ShardSessionRecord actual) {
            checkNotNull(actual);
            return new UpdateResult(null, actual);
        }

        /**
         * Tells whether the update was successful.
         */
        boolean isSuccessful() {
            return success != null;
        }

        /**
         * Returns a result of the update.
         *
         * <p>If the update was successful, returns the updated record.
         *
         * <p>Otherwise, returns the record currently residing in the storage.
         */
        ShardSessionRecord value() {
            if(success != null) {
                return success;
            }
            if(actual != null) {
                return actual;
            }
            throw newIllegalStateException(
                    "Transactional update of `ShardSessionRecord` must have a non-`null` result."
            );
        }
    }
}
