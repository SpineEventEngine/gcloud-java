/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.CompositeQueryParameter;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Streams.stream;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.entity.FieldMasks.applyMask;
import static io.spine.server.storage.datastore.DsIdentifiers.keyFor;
import static io.spine.server.storage.datastore.DsIdentifiers.ofEntityId;
import static io.spine.server.storage.datastore.Entities.activeEntity;
import static io.spine.server.storage.datastore.Entities.entityToMessage;
import static io.spine.server.storage.datastore.Entities.messageToEntity;
import static io.spine.validate.Validate.isDefault;
import static java.util.Collections.emptyIterator;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;

/**
 * {@link RecordStorage} implementation based on Google App Engine Datastore.
 *
 * @see DatastoreStorageFactory
 */
public class DsRecordStorage<I> extends RecordStorage<I> {

    private final DatastoreWrapper datastore;
    private final TypeUrl typeUrl;

    private final ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry;
    private final ColumnFilterAdapter columnFilterAdapter;
    private final Class<I> idClass;

    private static final TypeUrl RECORD_TYPE_URL = TypeUrl.of(EntityRecord.class);

    @SuppressWarnings("ReturnOfNull") // as annotated.
    private static final Function<@Nullable Entity, @Nullable EntityRecord> recordFromEntity =
            input -> {
                if (input == null) {
                    return null;
                }
                EntityRecord record = entityToMessage(input, RECORD_TYPE_URL);
                return record;
            };

    /**
     * Creates a new storage instance.
     * 
     * @param multitenant
     *         {@code true} if the storage supports multiple tenants,
     *         {@code false} for the single tenant storage
     * @param entityClass
     *         the class of entities stored in the storage
     * @param idClass
     *         the class of identifiers of stored entities
     * @param descriptor
     *         the descriptor of the type of messages to save to the storage
     * @param datastore
     *         the Datastore implementation to use
     * @param columnTypeRegistry
     *         the registry with column type information
     */
    @SuppressWarnings("ConstructorWithTooManyParameters")
    protected DsRecordStorage(
            boolean multitenant,
            Class<? extends io.spine.server.entity.Entity> entityClass,
            Class<I> idClass,
            Descriptor descriptor,
            DatastoreWrapper datastore,
            ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry) {
        super(multitenant, entityClass);
        this.typeUrl = TypeUrl.from(descriptor);
        this.datastore = datastore;
        this.columnTypeRegistry = checkNotNull(columnTypeRegistry);
        this.idClass = checkNotNull(idClass);
        this.columnFilterAdapter = ColumnFilterAdapter.of(this.columnTypeRegistry);
    }

    private DsRecordStorage(Builder<I> builder) {
        this(builder.isMultitenant(),
             builder.getEntityClass(),
             builder.getIdClass(),
             builder.getDescriptor(),
             builder.getDatastore(),
             builder.getColumnTypeRegistry()
        );
    }

    private Key keyOf(I id) {
        return keyFor(datastore, getKind(), ofEntityId(id));
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
    protected @Nullable Optional<EntityRecord> readRecord(I id) {
        Key key = keyOf(id);
        Entity response = datastore.read(key);

        if (response == null) {
            return empty();
        }

        EntityRecord result = entityToMessage(response, RECORD_TYPE_URL);
        return of(result);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return lookup(ids, recordFromEntity);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids, FieldMask fieldMask) {
        return lookup(ids, input -> entityToRecord(input, fieldMask));
    }

    private static
    @Nullable EntityRecord entityToRecord(@Nullable Entity input, FieldMask fieldMask) {
        if (input == null) {
            return null;
        }
        EntityRecord readRecord = entityToMessage(input, RECORD_TYPE_URL);
        Message state = unpack(readRecord.getState());
        Message maskedState = applyMask(fieldMask, state);
        Any wrappedState = AnyPacker.pack(maskedState);

        EntityRecord record = EntityRecord
                .newBuilder(readRecord)
                .setState(wrappedState)
                .build();
        return record;
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords() {
        Iterator<EntityRecord> result = readAllRecords(FieldMask.getDefaultInstance());
        return result;
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(FieldMask fieldMask) {
        StructuredQuery<Entity> allQuery = buildAllQuery(typeUrl);
        Iterator<EntityRecord> result = queryAll(allQuery, fieldMask, activeEntity());
        return result;
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(EntityQuery<I> query, FieldMask fieldMask) {
        if (query.getIds()
                 .isEmpty()
        && !query.getParameters()
                 .iterator()
                 .hasNext()) {
            return readAll(fieldMask);
        }
        return queryBy(query, fieldMask);
    }

    /**
     * Performs Datastore query by the given {@link EntityQuery}.
     *
     * <p>This method assumes that there are either IDs of query parameters or both in the given
     * {@code EntityQuery} (i.e. the query is not empty).
     *
     * @param entityQuery the {@link EntityQuery} to query the Datastore by
     * @param fieldMask   the {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> queryBy(EntityQuery<I> entityQuery, FieldMask fieldMask) {
        Collection<I> idFilter = entityQuery.getIds();
        QueryParameters params = entityQuery.getParameters();
        Iterator<EntityRecord> result;
        if (!idFilter.isEmpty()) {
            result = queryByIdsAndColumns(idFilter, params, fieldMask);
        } else {
            result = queryByColumnsOnly(params, fieldMask);
        }
        return result;
    }

    /**
     * Performs a query by IDs and entity columns.
     *
     * <p>The by-IDs query is performed on Datastore, and the by-columns filtering is done in
     * memory.
     *
     * @param acceptableIds the IDs to search by
     * @param params        the additional query parameters
     * @param fieldMask     the {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> queryByIdsAndColumns(Collection<I> acceptableIds,
                                                        QueryParameters params,
                                                        FieldMask fieldMask) {
        Predicate<Entity> inMemPredicate;
        if (params.iterator().hasNext()) { // IDs and columns query
            inMemPredicate = buildMemoryPredicate(params);
        } else { // Only IDs query
            inMemPredicate = entity -> true;
        }
        Iterator<EntityRecord> records = lookup(acceptableIds, fieldMask, inMemPredicate);
        return records;
    }

    /**
     * Performs a query by entity columns.
     *
     * <p>The query is performed on Datastore. A single call to this method may turn into several
     * API calls. See {@link DsFilters} for details.
     *
     * @param params    the by-column query parameters
     * @param fieldMask the {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> queryByColumnsOnly(QueryParameters params, FieldMask fieldMask) {
        StructuredQuery.Builder<Entity> datastoreQuery = Query.newEntityQueryBuilder()
                                                              .setKind(getKind().getValue());
        Collection<Filter> filters = buildColumnFilters(params);
        Collection<StructuredQuery<Entity>> queries =
                filters.stream()
                       .map(new FilterToQuery(datastoreQuery))
                       .collect(toList());
        Iterator<EntityRecord> result = queryAndMerge(queries, fieldMask);
        return result;
    }

    /**
     * Performs the given Datastore {@linkplain StructuredQuery queries} and combines results into
     * a single lazy iterator.
     *
     * <p>The resulting iterator is constructed of
     * {@linkplain DatastoreWrapper#read(StructuredQuery) Datastore query response iterators}
     * concatenated together one by one. Each of them is evaluated only after the previous one runs
     * out of records (i.e. {@code hasNext()} method returns {@code false}). The order of
     * the iterators corresponds to the order of the {@code queries}.
     *
     * @param queries   the queries to perform
     * @param fieldMask the {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> queryAndMerge(Iterable<StructuredQuery<Entity>> queries,
                                                 FieldMask fieldMask) {
        Iterator<EntityRecord> result = emptyIterator();
        for (StructuredQuery<Entity> query : queries) {
            Iterator<EntityRecord> records = queryAll(query, fieldMask, entity -> true);
            result = concat(result, records);
        }
        return result;
    }

    private Predicate<Entity> buildMemoryPredicate(QueryParameters params) {
        return new EntityColumnPredicate(params, columnFilterAdapter);
    }

    private Collection<Filter> buildColumnFilters(
            Iterable<CompositeQueryParameter> compositeParameters) {
        Collection<CompositeQueryParameter> params = newArrayList(compositeParameters);
        Collection<Filter> predicate = DsFilters.fromParams(params, columnFilterAdapter);
        return predicate;
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
    @VisibleForTesting // Otherwise this getter is not used
    TypeUrl getTypeUrl() {
        return typeUrl;
    }

    private Iterator<EntityRecord> lookup(Collection<I> ids,
                                          FieldMask fieldMask,
                                          Predicate<Entity> predicate) {
        if (ids.isEmpty()) {
            return readAllRecords(fieldMask);
        }
        Collection<Key> keys = toKeys(ids);
        Iterator<Entity> records = datastore.read(keys);
        Iterator<EntityRecord> result = toRecords(records, predicate, fieldMask);
        return result;
    }

    private <T> Iterator<T> lookup(Iterable<I> ids, Function<Entity, @Nullable T> transformer) {
        Collection<Key> keys = toKeys(ids);
        Iterator<Entity> results = datastore.read(keys);
        return stream(results).filter(activeEntity())
                              .map(transformer)
                              .iterator();
    }

    private Iterator<EntityRecord> queryAll(StructuredQuery<Entity> query,
                                            FieldMask fieldMask,
                                            Predicate<Entity> inMemFilter) {
        Iterator<Entity> results = datastore.read(query);
        return toRecords(results, inMemFilter, fieldMask);
    }

    protected final Iterator<EntityRecord> toRecords(Iterator<Entity> queryResults,
                                                     Predicate<Entity> filter,
                                                     FieldMask fieldMask) {
        Stream<Entity> filtered = stream(queryResults).filter(filter);

        Function<Entity, EntityRecord> applyFieldMask = new Function<Entity, EntityRecord>() {

            private final boolean maskNotEmpty = !isDefault(fieldMask);

            @Override
            public EntityRecord apply(Entity input) {
                checkNotNull(input);
                EntityRecord record = DsRecordStorage.this.getRecordFromEntity(input);
                if (maskNotEmpty) {
                    Message state = unpack(record.getState());
                    state = applyMask(fieldMask, state);
                    record = EntityRecord.newBuilder(record)
                                         .setState(AnyPacker.pack(state))
                                         .build();
                }
                return record;
            }
        };
        Iterator<EntityRecord> result = filtered.map(applyFieldMask)
                                                .iterator();
        return result;
    }

    protected Entity entityRecordToEntity(I id, EntityRecordWithColumns record) {
        EntityRecord entityRecord = record.getRecord();
        Key key = keyFor(datastore, kindFrom(entityRecord), ofEntityId(id));
        Entity incompleteEntity = messageToEntity(entityRecord, key);
        Entity.Builder entity = Entity.newBuilder(incompleteEntity);

        populateFromStorageFields(entity, record);

        Entity completeEntity = entity.build();
        return completeEntity;
    }

    protected void populateFromStorageFields(BaseEntity.Builder<Key, Entity.Builder> entity,
                                             EntityRecordWithColumns record) {
        if (record.hasColumns()) {
            ColumnRecords.feedColumnsTo(entity, record, columnTypeRegistry, Functions.identity());
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

    static Kind kindFrom(TypeUrl typeUrl) {
        return Kind.of(typeUrl);
    }

    protected Kind getKind() {
        return kindFrom(typeUrl);
    }

    EntityRecord getRecordFromEntity(Entity entity) {
        EntityRecord record = entityToMessage(entity, RECORD_TYPE_URL);
        return record;
    }

    /**
     * Constructs a Datastore {@link Query}, which fetches all the records of the given type.
     *
     * @param typeUrl the type of the records to fetch
     * @return new {@link StructuredQuery}
     */
    protected StructuredQuery<Entity> buildAllQuery(TypeUrl typeUrl) {
        Kind kind = kindFrom(typeUrl);
        StructuredQuery<Entity> query = newQuery(kind);
        return query;
    }

    private static com.google.cloud.datastore.EntityQuery newQuery(Kind kind) {
        return Query.newEntityQueryBuilder()
                    .setKind(kind.getValue())
                    .build();
    }

    private Collection<Key> toKeys(Iterable<I> ids) {
        Collection<Key> keys = newLinkedList();
        for (I id : ids) {
            Key key = keyFor(datastore, kindFrom(typeUrl), ofEntityId(id));
            keys.add(key);
        }
        return keys;
    }

    /**
     * A function transforming the input {@link Filter} into a {@link StructuredQuery} with
     * the given newBuilder.
     */
    private static final class FilterToQuery implements Function<Filter, StructuredQuery<Entity>> {

        private final StructuredQuery.Builder<Entity> builder;

        private FilterToQuery(StructuredQuery.Builder<Entity> builder) {
            this.builder = builder;
        }

        @Override
        public StructuredQuery<Entity> apply(@Nullable Filter filter) {
            checkNotNull(filter);
            StructuredQuery<Entity> query = builder.setFilter(filter)
                                                   .build();
            return query;
        }
    }

    /**
     * Creates new instance of the {@link Builder}.
     *
     * @param <I> the ID type of the instances built by the created {@link Builder}
     * @return new instance of the {@link Builder}
     */
    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * A newBuilder for the {@code DsRecordStorage}.
     */
    public static final class Builder<I> extends AbstractBuilder<I, Builder<I>> {

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
            super();
        }

        /**
         * Creates new instance of the {@code DsRecordStorage}.
         */
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
