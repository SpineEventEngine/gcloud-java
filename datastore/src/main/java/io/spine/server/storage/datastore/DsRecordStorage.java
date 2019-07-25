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

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.client.OrderBy;
import io.spine.client.ResponseFormat;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.datastore.type.DatastoreColumnType;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.entity.storage.ColumnRecords.feedColumnsTo;
import static io.spine.server.entity.storage.QueryParameters.activeEntityQueryParams;
import static io.spine.server.storage.datastore.Entities.RECORD_TYPE_URL;
import static io.spine.server.storage.datastore.Entities.builderFromMessage;
import static io.spine.server.storage.datastore.Entities.toMessage;
import static io.spine.server.storage.datastore.RecordId.ofEntityId;
import static java.util.Optional.empty;

/**
 * {@link RecordStorage} implementation based on Google Cloud Datastore.
 *
 * @see DatastoreStorageFactory
 */
public class DsRecordStorage<I> extends RecordStorage<I> {

    private final DatastoreWrapper datastore;
    private final Class<I> idClass;
    private final TypeUrl typeUrl;

    private final DsLookupByIds<I> idLookup;
    private final DsLookupByQueries queryLookup;

    private final ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry;
    private final FilterAdapter columnFilterAdapter;

    /**
     * Creates new {@link Builder} instance.
     *
     * @param <I>
     *         the ID type of the instances built by the created {@link Builder}
     * @return new instance of the {@link Builder}
     */
    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * Creates new instance by the passed builder.
     */
    protected DsRecordStorage(
            RecordStorageBuilder<I, ? extends RecordStorage, ? extends RecordStorageBuilder> b) {
        super(b.isMultitenant(), b.getEntityClass());
        this.typeUrl = TypeUrl.from(b.getDescriptor());
        this.idClass = checkNotNull(b.getIdClass());
        this.datastore = b.getDatastore();
        this.columnTypeRegistry = checkNotNull(b.getColumnTypeRegistry());
        this.columnFilterAdapter = FilterAdapter.of(this.columnTypeRegistry);
        this.idLookup = new DsLookupByIds<>(this.datastore, this.typeUrl);
        this.queryLookup = new DsLookupByQueries(this.datastore, this.typeUrl,
                                                 this.columnFilterAdapter);
    }

    private Key keyOf(I id) {
        return datastore.keyFor(getKind(), ofEntityId(id));
    }

    @Override
    public boolean delete(I id) {
        Key key = keyOf(id);
        datastore.delete(key);

        // Check presence
        Entity record = datastore.read(key);
        return record == null;
    }

    @Override
    protected Optional<EntityRecord> readRecord(I id) {
        Key key = keyOf(id);
        Entity response = datastore.read(key);

        if (response == null) {
            return empty();
        }

        EntityRecord result = toMessage(response, RECORD_TYPE_URL);
        return Optional.of(result);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(ResponseFormat format) {
        Iterator<EntityRecord> result = queryLookup.find(activeEntityQueryParams(this), format);
        return result;
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(EntityQuery<I> query, ResponseFormat format) {
        if (isQueryForAll(query)) {
            return readAll(format);
        }
        return queryBy(query, format);
    }

    @Override
    protected Iterator<@Nullable EntityRecord> readMultipleRecords(Iterable<I> ids,
                                                                   FieldMask fieldMask) {
        return idLookup.findActive(ids, fieldMask);
    }

    @SuppressWarnings("PMD.SimplifyBooleanReturns")
    // Cleaner with each rule out condition stated explicitly.
    private static <I> boolean isQueryForAll(EntityQuery<I> query) {
        if (!query.getIds()
                  .isEmpty()) {
            return false;
        }

        QueryParameters params = query.getParameters();
        return !notEmpty(params);
    }

    /**
     * Performs Datastore query by the given {@link EntityQuery}.
     *
     * <p>This method assumes that there are either IDs of query parameters or both in the given
     * {@code EntityQuery} (i.e. the query is not empty).
     *
     * @param entityQuery
     *         the {@link EntityQuery} to query the Datastore by
     * @param responseFormat
     *         the {@code ResponseFormat} according to which the result is retrieved
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> queryBy(EntityQuery<I> entityQuery,
                                           ResponseFormat responseFormat) {
        EntityQuery<I> completeQuery = includeLifecycle(entityQuery);
        Collection<I> idFilter = completeQuery.getIds();
        QueryParameters params = completeQuery.getParameters();
        Iterator<EntityRecord> result = idFilter.isEmpty()
                                        ? queryLookup.find(params, responseFormat)
                                        : queryByIdsAndColumns(idFilter, params, responseFormat);
        return result;
    }

    private EntityQuery<I> includeLifecycle(EntityQuery<I> entityQuery) {
        return !entityQuery.isLifecycleAttributesSet()
               ? entityQuery.withActiveLifecycle(this)
               : entityQuery;
    }

    /**
     * Performs a query by IDs and entity columns.
     *
     * <p>The by-IDs query is performed on Datastore, and the by-columns filtering is done in
     * memory.
     *
     * @param acceptableIds
     *         the IDs to search by
     * @param params
     *         the additional query parameters
     * @param format
     *         the format of the response including a field mask to apply, response size limit
     *         and ordering
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> queryByIdsAndColumns(Collection<I> acceptableIds,
                                                        QueryParameters params,
                                                        ResponseFormat format) {
        Predicate<Entity> inMemPredicate = columnPredicate(params);
        FieldMask fieldMask = format.getFieldMask();
        if (format.hasOrderBy()) {
            OrderBy order = format.getOrderBy();
            int limit = format.getLimit();
            if (limit > 0) {
                return idLookup.find(acceptableIds, fieldMask, inMemPredicate, order, limit);
            }
            return idLookup.find(acceptableIds, fieldMask, inMemPredicate, order);
        }
        return idLookup.find(acceptableIds, fieldMask, inMemPredicate);
    }

    private Predicate<Entity> columnPredicate(QueryParameters params) {
        if (notEmpty(params)) {
            return new EntityColumnPredicate(params, columnFilterAdapter);
        }
        return entity -> true;
    }

    private static boolean notEmpty(QueryParameters params) {
        return params.iterator()
                     .hasNext();
    }

    /**
     * Provides an access to the GAE Datastore with an API, specific to the Spine framework.
     *
     * <p>Allows the customization of the storage behavior in descendants.
     *
     * @return the wrapped instance of Datastore
     */
    protected DatastoreWrapper getDatastore() {
        return datastore;
    }

    /**
     * Obtains the {@link TypeUrl} of the messages to save to this store.
     *
     * <p>Allows the customization of the storage behavior in descendants.
     *
     * @return the {@link TypeUrl} of the stored messages
     */
    @VisibleForTesting
    // Otherwise this getter is not used
    TypeUrl getTypeUrl() {
        return typeUrl;
    }

    protected Entity entityRecordToEntity(I id, EntityRecordWithColumns record) {
        EntityRecord entityRecord = record.getRecord();
        Key key = datastore.keyFor(kindFrom(entityRecord), ofEntityId(id));
        Entity.Builder entity = builderFromMessage(entityRecord, key);

        populateFromStorageFields(entity, record);

        Entity completeEntity = entity.build();
        return completeEntity;
    }

    private void populateFromStorageFields(BaseEntity.Builder<Key, Entity.Builder> entity,
                                           EntityRecordWithColumns record) {
        if (record.hasColumns()) {
            feedColumnsTo(entity, record, columnTypeRegistry, Functions.identity());
        }
    }

    @Override
    protected void writeRecord(I id, EntityRecordWithColumns entityStorageRecord) {
        checkNotNull(id, "ID is null.");
        checkNotNull(entityStorageRecord, "Message is null.");

        Entity entity = entityRecordToEntity(id, entityStorageRecord);
        datastore.createOrUpdate(entity);
    }

    @Override
    protected void writeRecords(Map<I, EntityRecordWithColumns> records) {
        checkNotNull(records);

        Collection<Entity> entitiesToWrite = new ArrayList<>(records.size());
        for (Entry<I, EntityRecordWithColumns> record : records.entrySet()) {
            Entity entity = entityRecordToEntity(record.getKey(), record.getValue());
            entitiesToWrite.add(entity);
        }
        datastore.createOrUpdate(entitiesToWrite);
    }

    @Override
    public Iterator<I> index() {
        checkNotClosed();
        return Indexes.indexIterator(datastore, getKind(), idClass);
    }

    private static Kind kindFrom(EntityRecord record) {
        Any packedState = record.getState();
        Message state = unpack(packedState);
        Kind kind = Kind.of(state);
        return kind;
    }

    protected Kind getKind() {
        return Kind.of(typeUrl);
    }

    /**
     * A newBuilder for the {@code DsRecordStorage}.
     */
    public static final class Builder<I>
            extends RecordStorageBuilder<I, DsRecordStorage<I>, Builder<I>> {

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
            super();
        }

        /**
         * Creates new instance of the {@code DsRecordStorage}.
         */
        @Override
        public DsRecordStorage<I> build() {
            checkRequiredFields();
            DsRecordStorage<I> storage = new DsRecordStorage<>(this);
            return storage;
        }

        @Override
        Builder<I> self() {
            return this;
        }
    }
}
