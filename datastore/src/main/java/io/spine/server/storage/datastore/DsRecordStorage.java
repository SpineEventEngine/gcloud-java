/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Multimap;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.client.ColumnFilter;
import io.spine.client.CompositeColumnFilter;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.datastore.type.DatastoreColumnType;
import io.spine.string.Stringifiers;
import io.spine.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.entity.FieldMasks.applyMask;
import static io.spine.server.storage.OperatorEvaluator.eval;
import static io.spine.server.storage.datastore.DsIdentifiers.keyFor;
import static io.spine.server.storage.datastore.DsIdentifiers.ofEntityId;
import static io.spine.server.storage.datastore.Entities.activeEntity;
import static io.spine.util.Exceptions.newIllegalArgumentException;
import static io.spine.validate.Validate.isDefault;
import static java.util.Collections.emptyIterator;

/**
 * {@link RecordStorage} implementation based on Google App Engine Datastore.
 *
 * @author Alexander Litus
 * @author Dmytro Dashenkov
 * @see DatastoreStorageFactory
 */
public class DsRecordStorage<I> extends RecordStorage<I> {

    private final DatastoreWrapper datastore;
    private final TypeUrl typeUrl;

    private final ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry;
    private final ColumnFilterAdapter columnFilterAdapter;
    private final Class<I> idClass;

    private static final TypeUrl RECORD_TYPE_URL = TypeUrl.of(EntityRecord.class);

    private static final Function<Entity, EntityRecord> recordFromEntity
            = new Function<Entity, EntityRecord>() {
        @Nullable
        @Override
        public EntityRecord apply(@Nullable Entity input) {
            if (input == null) {
                return null;
            }

            final EntityRecord record = Entities.entityToMessage(input, RECORD_TYPE_URL);
            return record;
        }
    };

    /**
     * Creates a new storage instance.
     *
     * @param descriptor the descriptor of the type of messages to save to the storage
     * @param datastore  the Datastore implementation to use
     */
    protected DsRecordStorage(
            Descriptor descriptor,
            DatastoreWrapper datastore,
            boolean multitenant,
            ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry,
            Class<I> idClass) {
        super(multitenant);
        this.typeUrl = TypeUrl.from(descriptor);
        this.datastore = datastore;
        this.columnTypeRegistry = checkNotNull(columnTypeRegistry);
        this.idClass = checkNotNull(idClass);
        this.columnFilterAdapter = ColumnFilterAdapter.of(this.columnTypeRegistry);
    }

    private DsRecordStorage(Builder<I> builder) {
        this(builder.descriptor,
             builder.datastore,
             builder.multitenant,
             builder.columnTypeRegistry,
             builder.idClass);
    }

    @Override
    public boolean delete(I id) {
        final Key key = keyFor(datastore,
                               getKind(),
                               ofEntityId(id));
        datastore.delete(key);

        // Check presence
        final Entity record = datastore.read(key);
        return record == null;
    }

    @Nullable
    @Override
    protected Optional<EntityRecord> readRecord(I id) {
        final Key key = keyFor(datastore,
                               getKind(),
                               ofEntityId(id));
        final Entity response = datastore.read(key);

        if (response == null) {
            return Optional.absent();
        }

        final EntityRecord result = Entities.entityToMessage(response, RECORD_TYPE_URL);
        return Optional.of(result);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return lookup(ids, recordFromEntity);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids,
                                                         final FieldMask fieldMask) {
        final Function<Entity, EntityRecord> transformer = new Function<Entity, EntityRecord>() {
            @Nullable
            @Override
            public EntityRecord apply(@Nullable Entity input) {
                if (input == null) {
                    return null;
                }

                final EntityRecord readRecord = Entities.entityToMessage(input, RECORD_TYPE_URL);
                final Message state = unpack(readRecord.getState());
                final TypeUrl typeUrl = TypeUrl.from(state.getDescriptorForType());
                final Message maskedState = applyMask(fieldMask, state, typeUrl);
                final Any wrappedState = AnyPacker.pack(maskedState);

                final EntityRecord record = EntityRecord.newBuilder(readRecord)
                                                        .setState(wrappedState)
                                                        .build();
                return record;
            }
        };

        return lookup(ids, transformer);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords() {
        return readAllRecords(FieldMask.getDefaultInstance());
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(final FieldMask fieldMask) {
        final StructuredQuery<Entity> allQuery = buildAllQuery(typeUrl);
        return queryAll(typeUrl, allQuery, fieldMask);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(EntityQuery<I> query, FieldMask fieldMask) {
        if (query.getIds().isEmpty() && !query.getParameters().iterator().hasNext()) {
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
        final Collection<I> idFilter = entityQuery.getIds();
        final QueryParameters params = entityQuery.getParameters();
        final Iterator<EntityRecord> result;
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
        final Predicate<Entity> inMemPredicate;
        if (params.iterator().hasNext()) { // IDs and columns query
            inMemPredicate = buildMemoryPredicate(params);
        } else { // Only IDs query
            inMemPredicate = alwaysTrue();
        }
        final Iterator<EntityRecord> records = lookup(acceptableIds, fieldMask, inMemPredicate);
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
    private Iterator<EntityRecord> queryByColumnsOnly(QueryParameters params,
                                                      FieldMask fieldMask) {
        final StructuredQuery.Builder<Entity> datastoreQuery = Query.newEntityQueryBuilder()
                                                                    .setKind(getKind().getValue());
        final Collection<Filter> filters = buildColumnFilters(params);
        final FilterToQuery function = new FilterToQuery(datastoreQuery);
        final Collection<StructuredQuery<Entity>> queries = transform(filters, function);
        final Iterator<EntityRecord> result = queryAndMerge(queries, fieldMask);
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
            final Iterator<EntityRecord> records = queryAll(typeUrl,
                                                            query,
                                                            fieldMask);
            result = concat(result, records);
        }
        return result;
    }

    private Predicate<Entity> buildMemoryPredicate(QueryParameters params) {
        return new EntityColumnPredicate(params, columnFilterAdapter);
    }

    private Collection<Filter> buildColumnFilters(
            Iterable<CompositeQueryParameter> compositeParameters) {
        final Collection<CompositeQueryParameter> params = newArrayList(compositeParameters);
        final Collection<Filter> predicate = DsFilters.fromParams(params, columnFilterAdapter);
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
        final Collection<Key> keys = toKeys(ids);
        final Iterator<Entity> records = datastore.read(keys);
        final Iterator<EntityRecord> result = toRecords(records,
                                                        predicate,
                                                        typeUrl,
                                                        fieldMask);
        return result;
    }

    private <T> Iterator<T> lookup(Iterable<I> ids,
                                   Function<Entity, T> transformer) {
        final Collection<Key> keys = toKeys(ids);
        final Iterator<Entity> results = datastore.read(keys);
        final Iterator<Entity> filteredResults = filter(results, activeEntity());
        final Iterator<T> records = transform(filteredResults, transformer);
        return records;
    }

    private Iterator<EntityRecord> queryAll(TypeUrl typeUrl,
                                            StructuredQuery<Entity> query,
                                            FieldMask fieldMask) {
        final Iterator<Entity> results = datastore.read(query);
        return toRecords(results, Predicates.<Entity>alwaysTrue(), typeUrl, fieldMask);
    }

    protected final Iterator<EntityRecord> toRecords(Iterator<Entity> queryResults,
                                             Predicate<Entity> filter,
                                             final TypeUrl typeUrl,
                                             final FieldMask fieldMask) {
        final Iterator<Entity> filtered = filter(queryResults, filter);
        final Function<Entity, EntityRecord> transformer = new Function<Entity, EntityRecord>() {
            @Override
            public EntityRecord apply(@Nullable Entity input) {
                checkNotNull(input);
                EntityRecord record = getRecordFromEntity(input);
                if (!isDefault(fieldMask)) {
                    Message state = unpack(record.getState());
                    state = applyMask(fieldMask, state, typeUrl);
                    record = EntityRecord.newBuilder(record)
                                         .setState(AnyPacker.pack(state))
                                         .build();
                }
                return record;
            }
        };
        final Iterator<EntityRecord> result = transform(filtered, transformer);
        return result;
    }

    protected Entity entityRecordToEntity(I id, EntityRecordWithColumns record) {
        final EntityRecord entityRecord = record.getRecord();
        final Key key = keyFor(datastore,
                               kindFrom(entityRecord),
                               ofEntityId(id));
        final Entity incompleteEntity = Entities.messageToEntity(entityRecord, key);
        final Entity.Builder entity = Entity.newBuilder(incompleteEntity);

        populateFromStorageFields(entity, record);

        final Entity completeEntity = entity.build();
        return completeEntity;
    }

    protected void populateFromStorageFields(BaseEntity.Builder<Key, Entity.Builder> entity,
                                             EntityRecordWithColumns record) {
        if (record.hasColumns()) {
            ColumnRecords.feedColumnsTo(entity,
                                        record,
                                        columnTypeRegistry,
                                        Functions.<String>identity());
        }
    }

    @Override
    protected void writeRecord(I id, EntityRecordWithColumns entityStorageRecord) {
        checkNotNull(id, "ID is null.");
        checkNotNull(entityStorageRecord, "Message is null.");

        final Entity entity = entityRecordToEntity(id, entityStorageRecord);
        datastore.createOrUpdate(entity);
    }

    @Override
    protected void writeRecords(Map<I, EntityRecordWithColumns> records) {
        checkNotNull(records);

        final Collection<Entity> entitiesToWrite = new ArrayList<>(records.size());
        for (Entry<I, EntityRecordWithColumns> record : records.entrySet()) {
            final Entity entity = entityRecordToEntity(record.getKey(), record.getValue());
            entitiesToWrite.add(entity);
        }
        datastore.createOrUpdate(entitiesToWrite);
    }

    @Override
    public Iterator<I> index() {
        checkNotClosed();
        return Indexes.indexIterator(datastore,
                                     getKind(),
                                     idClass);
    }

    @Nullable
    protected Kind getDefaultKind() {
        return null;
    }

    private Kind kindFrom(EntityRecord record) {
        final Kind defaultKind = getDefaultKind();
        if (defaultKind != null) {
            return defaultKind;
        }
        final Any packedState = record.getState();
        final Message state = unpack(packedState);
        final Kind kind = Kind.of(state);
        return kind;
    }

    final Kind kindFrom(TypeUrl typeUrl) {
        final Kind defaultKind = getDefaultKind();
        if (defaultKind != null) {
            return defaultKind;
        }
        return Kind.of(typeUrl);
    }

    protected Kind getKind() {
        return kindFrom(typeUrl);
    }

    I unpackKey(Entity entity) {
        final String stringId = entity.getKey()
                                      .getName();
        final I id = Stringifiers.fromString(stringId, idClass);
        return id;
    }

    EntityRecord getRecordFromEntity(Entity entity) {
        final EntityRecord record = Entities.entityToMessage(entity, RECORD_TYPE_URL);
        return record;
    }

    protected StructuredQuery<Entity> buildAllQuery(TypeUrl typeUrl) {
        final String entityKind = kindFrom(typeUrl).getValue();
        final StructuredQuery<Entity> query = Query.newEntityQueryBuilder()
                                                   .setKind(entityKind)
                                                   .build();
        return query;
    }

    private Collection<Key> toKeys(Iterable<I> ids) {
        final Collection<Key> keys = newLinkedList();
        for (I id : ids) {
            final Key key = keyFor(datastore,
                                   kindFrom(typeUrl),
                                   ofEntityId(id));
            keys.add(key);
        }
        return keys;
    }


    private static class EntityColumnPredicate implements Predicate<Entity> {

        private final ColumnFilterAdapter adapter;
        private final Iterable<CompositeQueryParameter> queryParams;

        private EntityColumnPredicate(Iterable<CompositeQueryParameter> queryParams,
                                      ColumnFilterAdapter adapter) {
            this.adapter = adapter;
            this.queryParams = queryParams;
        }

        @Override
        public boolean apply(@Nullable Entity entity) {
            if (entity == null) {
                return false;
            }
            for (CompositeQueryParameter filter : queryParams) {
                final boolean match;
                final CompositeColumnFilter.CompositeOperator operator = filter.getOperator();
                switch (operator) {
                    case ALL:
                        match = checkAll(filter.getFilters(), entity);
                        break;
                    case EITHER:
                        match = checkEither(filter.getFilters(), entity);
                        break;
                    case UNRECOGNIZED:      // Fall through to default strategy
                    case CCF_CO_UNDEFINED:  // for the `default` and `faulty` enum values.
                    default:
                        throw newIllegalArgumentException("Composite operator %s is invalid.",
                                                          operator);
                }
                if (!match) {
                    return false;
                }
            }
            return true;
        }

        private boolean checkAll(Multimap<EntityColumn, ColumnFilter> filters, Entity entity) {
            for (Entry<EntityColumn, ColumnFilter> filter : filters.entries()) {
                final EntityColumn column = filter.getKey();
                final boolean matches = checkSingleParam(filter.getValue(), entity, column);
                if (!matches) {
                    return false;
                }
            }
            return true;
        }

        private boolean checkEither(Multimap<EntityColumn, ColumnFilter> filters, Entity entity) {
            for (Entry<EntityColumn, ColumnFilter> filter : filters.entries()) {
                final EntityColumn column = filter.getKey();
                final boolean matches = checkSingleParam(filter.getValue(), entity, column);
                if (matches) {
                    return true;
                }
            }
            return filters.isEmpty();
        }

        private boolean checkSingleParam(ColumnFilter filter, Entity entity, EntityColumn column) {
            final String columnName = column.getName();
            if (!entity.contains(columnName)) {
                return false;
            }
            final Object actual = entity.getValue(columnName).get();
            final Object expected = adapter.toValue(column, filter).get();

            final boolean result = eval(actual, filter.getOperator(), expected);
            return result;
        }
    }

    /**
     * A function transforming the input {@link Filter} into a {@link StructuredQuery} with
     * the given builder.
     */
    private static class FilterToQuery implements Function<Filter, StructuredQuery<Entity>> {

        private final StructuredQuery.Builder<Entity> builder;

        private FilterToQuery(StructuredQuery.Builder<Entity> builder) {
            this.builder = builder;
        }

        @Override
        public StructuredQuery<Entity> apply(@Nullable Filter filter) {
            checkNotNull(filter);
            final StructuredQuery<Entity> query = builder.setFilter(filter)
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
     * A builder for the {@code DsRecordStorage}.
     */
    public static class Builder<I> {

        private Descriptor descriptor;
        private DatastoreWrapper datastore;
        private boolean multitenant;
        private ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry;
        private Class<I> idClass;

        private Builder() {
            // Prevent direct initialization.
        }

        public Builder<I> setStateType(TypeUrl stateTypeUrl) {
            checkNotNull(stateTypeUrl);
            final Descriptor descriptor = (Descriptor) stateTypeUrl.getDescriptor();
            this.descriptor = checkNotNull(descriptor);
            return this;
        }

        /**
         * @param datastore the {@link DatastoreWrapper} to use in this storage
         */
        public Builder<I> setDatastore(DatastoreWrapper datastore) {
            this.datastore = checkNotNull(datastore);
            return this;
        }

        /**
         * @param multitenant {@code true} if the storage should be
         *                    {@link io.spine.server.storage.Storage#isMultitenant multitenant}
         *                    or not
         */
        public Builder<I> setMultitenant(boolean multitenant) {
            this.multitenant = multitenant;
            return this;
        }

        /**
         * @param columnTypeRegistry the type registry of the
         *                           {@linkplain io.spine.server.entity.storage.EntityColumn
         *                           entity columns}
         */
        public Builder<I> setColumnTypeRegistry(
                ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry) {
            this.columnTypeRegistry = checkNotNull(columnTypeRegistry);
            return this;
        }

        public Builder<I> setIdClass(Class<I> idClass) {
            this.idClass = checkNotNull(idClass);
            return this;
        }

        /**
         * Creates new instance of the {@code DsRecordStorage}.
         */
        public DsRecordStorage<I> build() {
            checkNotNull(descriptor, "State descriptor is not set.");
            checkNotNull(datastore, "Datastore is not set.");
            checkNotNull(columnTypeRegistry, "Column type registry is not set.");
            final DsRecordStorage<I> storage = new DsRecordStorage<>(this);
            return storage;
        }
    }
}
