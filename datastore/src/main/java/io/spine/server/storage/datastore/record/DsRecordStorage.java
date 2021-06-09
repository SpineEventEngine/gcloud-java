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

package io.spine.server.storage.datastore.record;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import io.spine.query.RecordQuery;
import io.spine.server.storage.ColumnMapping;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordWithColumns;
import io.spine.server.storage.datastore.DatastoreMedium;
import io.spine.server.storage.datastore.DatastoreStorageFactory;
import io.spine.server.storage.datastore.DatastoreWrapper;
import io.spine.server.storage.datastore.Kind;
import io.spine.server.storage.datastore.TransactionWrapper;
import io.spine.server.storage.datastore.config.StorageConfiguration;
import io.spine.server.storage.datastore.config.TxSetting;
import io.spine.server.storage.datastore.query.DsLookup;
import io.spine.server.storage.datastore.query.FilterAdapter;
import io.spine.type.TypeUrl;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterators.transform;
import static io.spine.server.storage.datastore.record.Entities.builderFromMessage;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * {@link RecordStorage} implementation based on Google Cloud Datastore.
 *
 * @see DatastoreStorageFactory
 */
public class DsRecordStorage<I, R extends Message> extends RecordStorage<I, R> {

    private final DatastoreWrapper datastore;
    private final TypeUrl typeUrl;
    private final Kind kind;
    private final DsEntitySpec<I, R> dsSpec;
    private final FilterAdapter columnFilterAdapter;
    private final ColumnMapping<Value<?>> columnMapping;
    private final TxSetting txSetting;

    public DsRecordStorage(StorageConfiguration<I, R> config) {
        super(config.context(), config.recordSpec()
                                      .recordSpec());
        this.datastore = config.datastore();
        columnMapping = config.columnMapping();
        this.columnFilterAdapter = FilterAdapter.of(columnMapping);
        this.txSetting = config.txSetting();
        this.dsSpec = config.recordSpec();
        this.kind = dsSpec.kind();
        this.typeUrl = TypeUrl.of(config.storedType());
    }

//    /**
//     * Creates new {@link Builder} instance.
//     *
//     * @param <I>
//     *         the ID type of the instances built by the created {@link Builder}
//     * @return new instance of the {@link Builder}
//     */
//    public static <I> Builder<I> newBuilder() {
//        return new Builder<>();
//    }

//    /**
//     * Creates new instance by the passed builder.
//     */
//    protected DsRecordStorage(
//            RecordStorageBuilder<I,
//                    ? extends RecordStorage<I>,
//                    ? extends RecordStorageBuilder<I, ? extends RecordStorage<I>, ?>> b
//    ) {
//        super(b.getEntityClass(), b.isMultitenant());
//        this.typeUrl = TypeUrl.from(b.getDescriptor());
//        this.idClass = checkNotNull(b.getIdClass());
//        this.datastore = b.getDatastore();
//        this.columnMapping = checkNotNull(b.getColumnMapping());
//        this.columnFilterAdapter = FilterAdapter.of(this.columnMapping);
//        this.idLookup = new DsLookupByIds<I, R>(this.datastore, this.typeUrl, adapter);
//        this.queryLookup =
//                new DsLookupByQueries<I, R>(this.datastore, this.columnFilterAdapter, this.typeUrl);
//    }

    @Override
    public Iterator<I> index() {
        //TODO:2021-02-10:alex.tymchenko: can we use transactions for key-only queries?
        checkNotClosed();
        return Indexes.indexIterator(datastore, kind(), recordSpec().idType());
    }

    @Override
    protected Iterator<I> index(RecordQuery<I, R> query) {
        RecordSpec<I, R, ?> spec = recordSpec();
        Iterator<R> recordIterator = readAllRecords(query);
        Iterator<I> result = transform(recordIterator, spec::idFromRecord);
        return result;
    }

    @Override
    public void write(I id, R record) {
        writeRecord(RecordWithColumns.of(id, record));
    }

    @Override
    protected void writeRecord(RecordWithColumns<I, R> record) {
        checkNotNull(record, "Record is null.");
        Entity entity = entityRecordToEntity(record);
        write((storage) -> storage.createOrUpdate(entity));
    }

    @Override
    protected void writeAllRecords(Iterable<? extends RecordWithColumns<I, R>> records) {
        checkNotNull(records);

        ImmutableList.Builder<Entity> entitiesToWrite = ImmutableList.<Entity>builder();
        for (RecordWithColumns<I, R> record : records) {
            Entity entity = entityRecordToEntity(record);
            entitiesToWrite.add(entity);
        }
        ImmutableList<Entity> prepared = entitiesToWrite.build();
        write((storage) -> datastore.createOrUpdate(prepared));
    }

    @Override
    protected Iterator<R> readAllRecords(RecordQuery<I, R> query) {
        Iterable<R> result =
                read((storage) -> DsLookup.onTopOf(datastore, columnFilterAdapter, dsSpec)
                                          .with(query)
                                          .readRecords());
        return result.iterator();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Always returns {@code true}.
     */
    @CanIgnoreReturnValue
    @Override
    protected boolean deleteRecord(I id) {
        Key key = keyOf(id);
        write(storage -> storage.delete(key));
        return true;
    }

    /**
     * Returns the kind of Datastore Entity.
     */
    @VisibleForTesting
    public final Kind kind() {
        return kind;
    }

    protected Key keyOf(I id) {
        Key result = dsSpec.keyOf(id, datastore);
        return result;
    }

    protected Entity entityRecordToEntity(RecordWithColumns<I, R> recordWithCols) {
        R record = recordWithCols.record();
        I id = recordWithCols.id();
        Key key = keyOf(id);
        Entity.Builder entity = builderFromMessage(record, key);

        recordWithCols.columnNames()
                      .forEach(columnName -> {
                          Value<?> columnValue = recordWithCols.columnValue(columnName,
                                                                            columnMapping);
                          entity.set(columnName.value(), columnValue);
                      });

        Entity completeEntity = entity.build();
        return completeEntity;
    }

    //////////// Transactional work:

    //    protected final TypeUrl typeUrl() {
//        return typeUrl;
//    }
//
//
//    protected final Entity toEntity(R record) {
//
//    }
//
    protected TransactionWrapper newTransaction() {
        return datastore.newTransaction();
    }

    @SuppressWarnings("OverlyBroadCatchBlock")  /* Treating all exceptions similarly. */
    protected void writeTransactionally(RecordWithColumns<I, R> record) {
        checkNotNull(record);

        try (TransactionWrapper tx = newTransaction()) {
            Entity entity = entityRecordToEntity(record);
            tx.createOrUpdate(entity);
            tx.commit();
        } catch (RuntimeException e) {
            throw newIllegalStateException(e,
                                           "Error writing a `%s` in a transaction.",
                                           record.getClass()
                                                 .getName());
        }
    }

    protected Optional<R> readTransactionally(I id) {
        Key key = keyOf(id);
        try (TransactionWrapper tx = datastore.newTransaction()) {
            Optional<Entity> result = tx.read(key);
            tx.commit();
            return result.map(this::toRecord);
        }
    }

    protected final R toRecord(Entity entity) {
        return Entities.toMessage(entity, typeUrl);
    }

    private <V> V read(ReadOperation<V> operation) {
        if (txSetting.txEnabled()) {
            try (TransactionWrapper tx = newTransaction()) {
                V result = operation.perform(tx);
                tx.commit();
                return result;
            } catch (RuntimeException e) {
                throw newIllegalStateException(e, "" +
                        "Error executing `ReadOperation` transactionally.");
            }
        } else {
            V result = operation.perform(datastore);
            return result;
        }
    }

    private void write(WriteOperation operation) {
        if (txSetting.txEnabled()) {
            try (TransactionWrapper tx = newTransaction()) {
                operation.perform(tx);
                tx.commit();
            } catch (RuntimeException e) {
                throw newIllegalStateException(e, "" +
                        "Error executing `WriteOperation` transactionally.");
            }
        } else {
            DatastoreMedium storage = datastore;
            operation.perform(storage);

        }
    }

    //////////// New code:

//    protected Key keyOf(I id) {
//        Key result = datastore.keyFor(kind, RecordId.ofEntityId(id));
//        return result;
//    }
//
//    @Override
//    public Iterator<I> index() {
//        //TODO:2021-02-10:alex.tymchenko: can we use transactions for key-only queries?
//        checkNotClosed();
//        return Indexes.indexIterator(datastore, getKind(), recordSpec().idType());
//    }
//
//    @Override
//    public void write(I id, R record) {
//        writeRecord(RecordWithColumns.of(id, record));
//    }
//
//    @Override
//    public Optional<R> read(I id) {
//        Key key = keyOf(id);
//        Optional<Entity> response = datastore.read(key);
//        Optional<R> record = response.map(this::toRecord);
//        return record;
//    }
//
//    protected Optional<R> readTransactionally(I id) {
//        Key key = keyOf(id);
//        try (TransactionWrapper tx = datastore.newTransaction()) {
//            Optional<Entity> result = tx.read(key);
//            tx.commit();
//            return result.map(this::toRecord);
//        }
//    }
//
//    @Override
//    protected Optional<R> read(I id, FieldMask mask) {
//        Optional<R> nonMasked = read(id);
//        Optional<R> result = nonMasked.map(FieldMaskApplier.recordMasker(mask));
//        return result;
//    }
//
//    @Override
//    protected Iterator<I> index(RecordQuery<I, R> query) {
//        RecordSpec<I, R, ?> spec = recordSpec();
//        Iterator<R> recordIterator = readAllRecords(query);
//        Iterator<I> result = transform(recordIterator, spec::idFromRecord);
//        return result;
//    }
//
//    @Override
//    protected void writeRecord(RecordWithColumns<I, R> record) {
//        checkNotNull(record, "Record is null.");
//        Entity entity = entityRecordToEntity(record);
//        datastore.createOrUpdate(entity);
//    }
//
//    @Override
//    protected void writeAllRecords(Iterable<? extends RecordWithColumns<I, R>> records) {
//        checkNotNull(records);
//
//        ImmutableList.Builder<Entity> entitiesToWrite = ImmutableList.<Entity>builder();
//        for (RecordWithColumns<I, R> record : records) {
//            Entity entity = entityRecordToEntity(record);
//            entitiesToWrite.add(entity);
//        }
//        ImmutableList<Entity> prepared = entitiesToWrite.build();
//        datastore.createOrUpdate(prepared);
//    }
//
//    @Override
//    protected Iterator<R> readAllRecords(RecordQuery<I, R> query) {
//        Iterable<R> result =
//                DsLookup.onTopOf(datastore, columnFilterAdapter)
//                        .with(query)
//                        .readRecords();
//        return result.iterator();
//    }
//
//    /**
//     * {@inheritDoc}
//     *
//     * <p>Always returns {@code true}.
//     */
//    @CanIgnoreReturnValue
//    @Override
//    protected boolean deleteRecord(I id) {
//        Key key = keyOf(id);
//        datastore.delete(key);
//        return true;
//    }
//
//    /**
//     * Provides an access to the Datastore with an API, specific to the Spine framework.
//     *
//     * <p>Allows the customization of the storage behavior in descendants.
//     *
//     * @return the wrapped instance of Datastore
//     */
//    protected DatastoreWrapper getDatastore() {
//        return datastore;
//    }
//
//    /**
//     * Obtains the {@link TypeUrl} of the messages to save to this store.
//     *
//     * <p>Allows the customization of the storage behavior in descendants.
//     *
//     * @return the {@link TypeUrl} of the stored messages
//     */
//    @VisibleForTesting  /* Otherwise this getter is not used. */
//    TypeUrl getTypeUrl() {
//        return typeUrl;
//    }
//
//    protected Entity entityRecordToEntity(RecordWithColumns<I, R> recordWithCols) {
//        R record = recordWithCols.record();
//        RecordId recordId = ofEntityId(recordWithCols.id());
//        Key key = datastore.keyFor(kind, recordId);
//        Entity.Builder entity = builderFromMessage(record, key);
//
//        //TODO:2021-01-29:alex.tymchenko: do we need that?
////        populateFromStorageFields(entity, recordWithCols);
//
//        Entity completeEntity = entity.build();
//        return completeEntity;
//    }
//
//    protected final Kind getKind() {
//        return kind;
//    }
//
//    protected final TypeUrl typeUrl() {
//        return typeUrl;
//    }
//
//    protected final R toRecord(Entity entity) {
//        return Entities.toMessage(entity, typeUrl);
//    }
//
//    protected final Entity toEntity(R record) {
//
//    }
//
//    protected TransactionWrapper newTransaction() {
//        return datastore.newTransaction();
//    }
//
//    protected void writeTransactionally(R record) {
//        checkNotNull(record);
//
//        try (TransactionWrapper tx = newTransaction()) {
//            Entity entity = toEntity(record);
//            tx.createOrUpdate(entity);
//            tx.commit();
//        } catch (RuntimeException e) {
//            throw newIllegalStateException(e,
//                                           "Error writing a `%s` in a transaction.",
//                                           record.getClass()
//                                                 .getName());
//        }
//    }
//

    //////////// LEGACY CODE:

//    @Override
//    protected Iterator<EntityRecord> readAllRecords(ResponseFormat format) {
//        Iterator<EntityRecord> result = queryLookup.find(activeEntityQueryParams(this), format);
//        return result;
//    }
//
//    @Override
//    protected Iterator<EntityRecord> readAllRecords(EntityQuery<I> query, ResponseFormat format) {
//        if (isQueryForAll(query)) {
//            return readAll(format);
//        }
//        return queryBy(query, format);
//    }
//
//    @Override
//    protected Iterator<@Nullable EntityRecord> readMultipleRecords(Iterable<I> ids,
//                                                                   FieldMask fieldMask) {
//        return idLookup.findActive(ids, fieldMask);
//    }

//    @SuppressWarnings("PMD.SimplifyBooleanReturns")
//    // Cleaner with each rule out condition stated explicitly.
//    private static <I> boolean isQueryForAll(EntityQuery<I> query) {
//        if (!query.getIds()
//                  .isEmpty()) {
//            return false;
//        }
//
//        QueryParameters params = query.getParameters();
//        return !notEmpty(params);
//    }

//    /**
//     * Performs Datastore query by the given {@link EntityQuery}.
//     *
//     * <p>This method assumes that there are either IDs of query parameters or both in the given
//     * {@code EntityQuery} (i.e. the query is not empty).
//     *
//     * @param entityQuery
//     *         the {@link EntityQuery} to query the Datastore by
//     * @param responseFormat
//     *         the {@code ResponseFormat} according to which the result is retrieved
//     * @return an iterator over the resulting entity records
//     */
//    private Iterator<EntityRecord> queryBy(EntityQuery<I> entityQuery,
//                                           ResponseFormat responseFormat) {
//        EntityQuery<I> completeQuery = includeLifecycle(entityQuery);
//        Collection<I> idFilter = completeQuery.getIds();
//        QueryParameters params = completeQuery.getParameters();
//        Iterator<EntityRecord> result = idFilter.isEmpty()
//                                        ? queryLookup.find(params, responseFormat)
//                                        : queryByIdsAndColumns(idFilter, params, responseFormat);
//        return result;
//    }

//    private EntityQuery<I> includeLifecycle(EntityQuery<I> entityQuery) {
//        return !entityQuery.isLifecycleAttributesSet()
//               ? entityQuery.withActiveLifecycle(this)
//               : entityQuery;
//    }
//
//    /**
//     * Performs a query by IDs and entity columns.
//     *
//     * <p>The by-IDs query is performed on Datastore, and the by-columns filtering is done in
//     * memory.
//     *
//     * @param acceptedIds
//     *         the IDs to search by
//     * @param params
//     *         the additional query parameters
//     * @param format
//     *         the format of the response including a field mask to apply, response size limit
//     *         and ordering
//     * @return an iterator over the resulting entity records
//     */
//    private Iterator<EntityRecord> queryByIdsAndColumns(Collection<I> acceptedIds,
//                                                        QueryParameters params,
//                                                        ResponseFormat format) {
//        Predicate<Entity> inMemPredicate = columnPredicate(params);
//        FieldMask fieldMask = format.getFieldMask();
//        if (format.hasOrderBy()) {
//            OrderBy order = format.getOrderBy();
//            int limit = format.getLimit();
//            if (limit > 0) {
//                return idLookup.find(acceptedIds, fieldMask, inMemPredicate, order, limit);
//            }
//            return idLookup.find(acceptedIds, fieldMask, inMemPredicate, order);
//        }
//        return idLookup.find(acceptedIds, fieldMask, inMemPredicate);
//    }
//
//    private Predicate<Entity> columnPredicate(QueryParameters params) {
//        if (notEmpty(params)) {
//            return new RecordColumnPredicate(params, columnFilterAdapter);
//        }
//        return entity -> true;
//    }
//
//    private static boolean notEmpty(QueryParameters params) {
//        return params.iterator()
//                     .hasNext();
//    }
//
//    @Override
//    protected void writeRecord(I id, EntityRecordWithColumns entityStorageRecord) {
//        checkNotNull(id, "ID is null.");
//        checkNotNull(entityStorageRecord, "Message is null.");
//
//        Entity entity = entityRecordToEntity(id, entityStorageRecord);
//        datastore.createOrUpdate(entity);
//    }
//
//    @Override
//    protected void writeRecords(Map<I, EntityRecordWithColumns> records) {
//        checkNotNull(records);
//
//        Collection<Entity> entitiesToWrite = new ArrayList<>(records.size());
//        for (Map.Entry<I, EntityRecordWithColumns> record : records.entrySet()) {
//            Entity entity = entityRecordToEntity(record.getKey(), record.getValue());
//            entitiesToWrite.add(entity);
//        }
//        datastore.createOrUpdate(entitiesToWrite);
//    }
//
//    private static Kind kindFrom(EntityRecord record) {
//        Any packedState = record.getState();
//        Message state = unpack(packedState);
//        Kind kind = Kind.of(state);
//        return kind;
//    }
}
