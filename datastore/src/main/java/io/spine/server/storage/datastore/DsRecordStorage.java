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
import com.google.common.collect.Multimap;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.client.ColumnFilter;
import io.spine.client.CompositeColumnFilter;
import io.spine.client.OrderBy;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.Storage;
import io.spine.server.storage.datastore.type.DatastoreColumnType;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.cloud.datastore.Query.newEntityQueryBuilder;
import static com.google.cloud.datastore.StructuredQuery.OrderBy.asc;
import static com.google.cloud.datastore.StructuredQuery.OrderBy.desc;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Streams.stream;
import static io.spine.client.OrderBy.Direction.ASCENDING;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.storage.OperatorEvaluator.eval;
import static io.spine.server.storage.datastore.DsIdentifiers.keyFor;
import static io.spine.server.storage.datastore.DsIdentifiers.ofEntityId;
import static io.spine.server.storage.datastore.DsQueryHelper.maskRecord;
import static io.spine.server.storage.datastore.Entities.activeEntity;
import static io.spine.server.storage.datastore.Entities.entityToMessage;
import static io.spine.util.Exceptions.newIllegalArgumentException;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;

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

    private final DsLookupById<I> idLookup;
    private final DsLookupByColumn<I> columnLookup;

    private final ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry;
    private final ColumnFilterAdapter columnFilterAdapter;
    private final Class<I> idClass;

    private static final TypeUrl RECORD_TYPE_URL = TypeUrl.of(EntityRecord.class);

    /**
     * Creates a new storage instance.
     *
     * @param descriptor
     *         the descriptor of the type of messages to save to the storage
     * @param datastore
     *         the Datastore implementation to use
     */
    protected DsRecordStorage(Descriptor descriptor, DatastoreWrapper datastore,
                              boolean multitenant,
                              ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry,
                              Class<I> idClass,
                              Class<? extends io.spine.server.entity.Entity> entityClass) {
        super(multitenant, entityClass);
        this.typeUrl = TypeUrl.from(descriptor);
        this.datastore = datastore;
        this.columnTypeRegistry = checkNotNull(columnTypeRegistry);
        this.idClass = checkNotNull(idClass);
        this.columnFilterAdapter = ColumnFilterAdapter.of(this.columnTypeRegistry);
        this.idLookup = new DsLookupById<>(this.datastore, this.typeUrl);
        this.columnLookup = new DsLookupByColumn<>(this.datastore, this.typeUrl);
    }

    private DsRecordStorage(Builder<I> builder) {
        this(builder.getDescriptor(),
             builder.getDatastore(),
             builder.isMultitenant(),
             builder.getColumnTypeRegistry(),
             builder.getIdClass(),
             builder.getEntityClass());
    }

    @Override
    public boolean delete(I id) {
        Key key = keyFor(datastore,
                         getKind(),
                         ofEntityId(id));
        datastore.delete(key);

        // Check presence
        Entity record = datastore.read(key);
        return record == null;
    }

    @Override
    protected @Nullable Optional<EntityRecord> readRecord(I id) {
        Key key = keyFor(datastore, getKind(), ofEntityId(id));
        Entity response = datastore.read(key);

        if (response == null) {
            return empty();
        }

        EntityRecord result = entityToMessage(response, RECORD_TYPE_URL);
        return of(result);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return idLookup.execute(ids);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids,
                                                         FieldMask fieldMask) {
        return idLookup.execute(ids, fieldMask);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords() {
        Iterator<EntityRecord> result = readAllRecords(FieldMask.getDefaultInstance());
        return result;
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(FieldMask fieldMask) {
        StructuredQuery<Entity> allQuery = buildAllQuery(typeUrl);
        Iterator<EntityRecord> result = queryAll(typeUrl,
                                                 allQuery,
                                                 fieldMask,
                                                 activeEntity());
        return result;
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(EntityQuery<I> query, FieldMask fieldMask) {
        if (isQueryForAll(query)) {
            return readAll(fieldMask);
        }
        return queryBy(query, fieldMask);
    }

    private static <I> boolean isQueryForAll(EntityQuery<I> query) {
        if (!query.getIds()
                  .isEmpty()) {
            return false;
        }

        QueryParameters params = query.getParameters();
        if (notEmpty(params)) {
            return false;
        }

        if (params.ordered()) {
            return false;
        }

        if (params.limited()) {
            return false;
        }

        return true;
    }

    /**
     * Performs Datastore query by the given {@link EntityQuery}.
     *
     * <p>This method assumes that there are either IDs of query parameters or both in the given
     * {@code EntityQuery} (i.e. the query is not empty).
     *
     * @param entityQuery
     *         the {@link EntityQuery} to query the Datastore by
     * @param fieldMask
     *         the {@code FieldMask} to apply to all the retrieved entity states
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
     * @param acceptableIds
     *         the IDs to search by
     * @param params
     *         the additional query parameters
     * @param fieldMask
     *         the {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> queryByIdsAndColumns(Collection<I> acceptableIds,
                                                        QueryParameters params,
                                                        FieldMask fieldMask) {
        Predicate<Entity> inMemPredicate = notEmpty(params) ? buildMemoryPredicate(params)
                                                            : (entity -> true);
        if (params.ordered()) {
            if (params.limited()) {
                return idLookup.execute(acceptableIds, fieldMask, inMemPredicate,
                                        params.orderBy(), params.limit());
            }
            return idLookup.execute(acceptableIds, fieldMask, inMemPredicate, params.orderBy());
        }
        return idLookup.execute(acceptableIds, fieldMask, inMemPredicate);
    }

    private static boolean notEmpty(QueryParameters params) {
        return params.iterator()
                     .hasNext();
    }

    /**
     * Performs a query by entity columns.
     *
     * <p>The query is performed on Datastore. A single call to this method may turn into several
     * API calls. See {@link DsFilters} for details.
     *
     * @param params
     *         the by-column query parameters
     * @param fieldMask
     *         the {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    private Iterator<EntityRecord> queryByColumnsOnly(QueryParameters params, FieldMask fieldMask) {
        StructuredQuery.Builder<Entity> datastoreQuery = constructDsQuery(params);
        Collection<StructuredQuery<Entity>> queries = splitQueriesByColumns(datastoreQuery, params);

        if (params.ordered()) {
            if (params.limited()) {
                return columnLookup.execute(queries, params.orderBy(), params.limit(), fieldMask);
            }
            return columnLookup.execute(queries, params.orderBy(), fieldMask);
        }
        return columnLookup.execute(queries, fieldMask);
    }

    private StructuredQuery.Builder<Entity> constructDsQuery(QueryParameters params) {
        StructuredQuery.Builder<Entity> datastoreQuery = newEntityQueryBuilder();
        datastoreQuery.setKind(getKind().getValue());

        if (params.ordered()) {
            orderQueryBy(params.orderBy(), datastoreQuery);
            if (params.limited()) {
                datastoreQuery.setLimit(params.limit());
            }
        }

        return datastoreQuery;
    }

    private List<StructuredQuery<Entity>>
    splitQueriesByColumns(StructuredQuery.Builder<Entity> datastoreQuery, QueryParameters params) {
        return buildColumnFilters(params)
                .stream()
                .map(new FilterToQuery(datastoreQuery))
                .collect(toList());
    }

    private static void
    orderQueryBy(OrderBy orderBy, StructuredQuery.Builder<Entity> datastoreQuery) {
        if (orderBy.getDirection() == ASCENDING) {
            datastoreQuery.setOrderBy(asc(orderBy.getColumn()));
        } else {
            datastoreQuery.setOrderBy(desc(orderBy.getColumn()));
        }
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
    @VisibleForTesting
    // Otherwise this getter is not used
    TypeUrl getTypeUrl() {
        return typeUrl;
    }

    private Iterator<EntityRecord> queryAll(TypeUrl typeUrl,
                                            StructuredQuery<Entity> query,
                                            FieldMask fieldMask,
                                            Predicate<Entity> inMemFilter) {
        Iterator<Entity> results = datastore.read(query);
        return stream(results)
                .filter(inMemFilter)
                .map(DsQueryHelper::toRecord)
                .map(maskRecord(typeUrl, fieldMask))
                .iterator();
    }

    protected Entity entityRecordToEntity(I id, EntityRecordWithColumns record) {
        EntityRecord entityRecord = record.getRecord();
        Key key = keyFor(datastore, kindFrom(entityRecord), ofEntityId(id));
        Entity incompleteEntity = Entities.messageToEntity(entityRecord, key);
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

    /**
     * Constructs a Datastore {@link Query}, which fetches all the records of the given type.
     *
     * @param typeUrl
     *         the type of the records to fetch
     * @return new {@link StructuredQuery}
     */
    protected StructuredQuery<Entity> buildAllQuery(TypeUrl typeUrl) {
        String entityKind = kindFrom(typeUrl).getValue();
        StructuredQuery<Entity> query = newEntityQueryBuilder().setKind(entityKind)
                                                               .build();
        return query;
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
        public boolean test(@Nullable Entity entity) {
            if (entity == null) {
                return false;
            }
            for (CompositeQueryParameter filter : queryParams) {
                boolean match;
                CompositeColumnFilter.CompositeOperator operator = filter.getOperator();
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
                EntityColumn column = filter.getKey();
                boolean matches = checkSingleParam(filter.getValue(), entity, column);
                if (!matches) {
                    return false;
                }
            }
            return true;
        }

        private boolean checkEither(Multimap<EntityColumn, ColumnFilter> filters, Entity entity) {
            for (Entry<EntityColumn, ColumnFilter> filter : filters.entries()) {
                EntityColumn column = filter.getKey();
                boolean matches = checkSingleParam(filter.getValue(), entity, column);
                if (matches) {
                    return true;
                }
            }
            return filters.isEmpty();
        }

        private boolean checkSingleParam(ColumnFilter filter, Entity entity, EntityColumn column) {
            String columnName = column.getName();
            if (!entity.contains(columnName)) {
                return false;
            }
            Object actual = entity.getValue(columnName)
                                  .get();
            Object expected = adapter.toValue(column, filter)
                                     .get();

            boolean result = eval(actual, filter.getOperator(), expected);
            return result;
        }
    }

    /**
     * A function transforming the input {@link Filter} into a {@link StructuredQuery} with
     * the given newBuilder.
     */
    private static class FilterToQuery implements Function<Filter, StructuredQuery<Entity>> {

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
     * @param <I>
     *         the ID type of the instances built by the created {@link Builder}
     * @return new instance of the {@link Builder}
     */
    public static <I> AbstractBuilder<I, Builder<I>> newBuilder() {
        return new Builder<>();
    }

    /**
     * A newBuilder for the {@code DsRecordStorage}.
     */
    public static final class Builder<I>
            extends AbstractBuilder<I, Builder<I>> {

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

    /**
     * An implementation base for {@code DsRecordStorage} builders.
     *
     * @param <I>
     *         the ID type of the stored entities
     * @param <B>
     *         the builder own type
     */
    protected abstract static class AbstractBuilder<I, B extends AbstractBuilder<I, B>> {

        private Descriptor descriptor;
        private DatastoreWrapper datastore;
        private boolean multitenant;
        private ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry;
        private Class<I> idClass;
        private Class<? extends io.spine.server.entity.Entity> entityClass;

        /**
         * Prevents direct instantiation.
         */
        AbstractBuilder() {
        }

        /**
         * @param stateTypeUrl
         *         the type URL of the entity state, which is stored in the resulting
         *         storage
         */
        public B setStateType(TypeUrl stateTypeUrl) {
            checkNotNull(stateTypeUrl);
            Descriptor descriptor = (Descriptor) stateTypeUrl.getDescriptor();
            this.descriptor = checkNotNull(descriptor);
            return self();
        }

        /**
         * @param datastore
         *         the {@link DatastoreWrapper} to use in this storage
         */
        public B setDatastore(DatastoreWrapper datastore) {
            this.datastore = checkNotNull(datastore);
            return self();
        }

        /**
         * @param multitenant
         *         {@code true} if the storage should be
         *         {@link Storage#isMultitenant multitenant}
         *         or not
         */
        public B setMultitenant(boolean multitenant) {
            this.multitenant = multitenant;
            return self();
        }

        /**
         * @param columnTypeRegistry
         *         the type registry of the
         *         {@linkplain EntityColumn
         *         entity columns}
         */
        public B setColumnTypeRegistry(
                ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry) {
            this.columnTypeRegistry = checkNotNull(columnTypeRegistry);
            return self();
        }

        /**
         * @param idClass
         *         the ID class of the stored entity
         */
        public B setIdClass(Class<I> idClass) {
            this.idClass = checkNotNull(idClass);
            return self();
        }

        /**
         * @param entityClass
         *         the class of the stored entity
         */
        public B setEntityClass(Class<? extends io.spine.server.entity.Entity> entityClass) {
            this.entityClass = checkNotNull(entityClass);
            return self();
        }

        /**
         * @return the {@link Descriptor} of the stored entity state type
         */
        public Descriptor getDescriptor() {
            return descriptor;
        }

        /**
         * @return the {@link DatastoreWrapper} used in this storage
         */
        public DatastoreWrapper getDatastore() {
            return datastore;
        }

        /**
         * @return {@code true} if the storage should be
         *         {@link Storage#isMultitenant multitenant} or not
         */
        public boolean isMultitenant() {
            return multitenant;
        }

        /**
         * @return the type registry of the {@linkplain EntityColumn
         *         entity columns}
         */
        public ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> getColumnTypeRegistry() {
            return columnTypeRegistry;
        }

        /**
         * @return the ID class of the stored entity
         */
        public Class<I> getIdClass() {
            return idClass;
        }

        /**
         * @return the class of the stored entity
         */
        public Class<? extends io.spine.server.entity.Entity> getEntityClass() {
            return entityClass;
        }

        final void checkRequiredFields() {
            checkNotNull(descriptor, "State descriptor is not set.");
            checkNotNull(datastore, "Datastore is not set.");
            checkNotNull(columnTypeRegistry, "Column type registry is not set.");
        }

        abstract B self();
    }
}
