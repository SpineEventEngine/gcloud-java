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
import com.google.common.collect.ImmutableMap;
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
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.datastore.type.DatastoreColumnType;
import io.spine.string.Stringifiers;
import io.spine.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.api.client.util.Maps.newHashMap;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.entity.FieldMasks.applyMask;
import static io.spine.server.storage.datastore.DsIdentifiers.keyFor;
import static io.spine.server.storage.datastore.DsIdentifiers.ofEntityId;
import static io.spine.server.storage.datastore.Entities.activeEntity;
import static io.spine.validate.Validate.isDefault;
import static java.util.Collections.unmodifiableCollection;

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
    private static final String ID_CONVERSION_ERROR_MESSAGE =
            "Entity had ID of an invalid type; could not parse ID from String. " +
            "Note: custom conversion is not supported. See io.spine.base.Identifier#idToString.";

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
    protected DsRecordStorage(Descriptor descriptor,
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
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return lookup(ids, recordFromEntity);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids,
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
    protected Map<I, EntityRecord> readAllRecords() {
        return readAllRecords(FieldMask.getDefaultInstance());
    }

    @Override
    protected Map<I, EntityRecord> readAllRecords(final FieldMask fieldMask) {
        final StructuredQuery<Entity> allQuery = buildAllQuery(typeUrl);
        return queryAll(typeUrl, allQuery, fieldMask);
    }

    @Override
    protected Map<I, EntityRecord> readAllRecords(EntityQuery<I> query, FieldMask fieldMask) {
        return queryByColumns(query, fieldMask);
    }

    private Map<I, EntityRecord> queryByColumns(EntityQuery<I> entityQuery, FieldMask fieldMask) {
        final StructuredQuery.Builder<Entity> datastoreQuery = Query.newEntityQueryBuilder()
                                                                    .setKind(getKind().getValue());
        final Iterable<CompositeQueryParameter> params = entityQuery.getParameters();
        final Collection<Filter> filters = buildColumnFilters(params);

        if (filters.isEmpty()) {
            return lookup(entityQuery.getIds(), fieldMask);
        }

        final Set<I> ids = entityQuery.getIds();

        final Collection<StructuredQuery<Entity>> queries = transform(
                filters,
                new Function<Filter, StructuredQuery<Entity>>() {
                    @Override
                    public StructuredQuery<Entity> apply(@Nullable Filter input) {
                        checkNotNull(input);
                        return datastoreQuery.setFilter(input)
                                             .build();
                    }
                }
        );

        final Predicate<Entity> inMemFilter = buildMemoryPredicate(ids);
        final Map<I, EntityRecord> result = newHashMap();
        for (StructuredQuery<Entity> query : queries) {
            final Map<I, EntityRecord> records = queryAll(typeUrl, query, fieldMask, inMemFilter);
            result.putAll(records);
        }
        return result;
    }

    private Predicate<Entity> buildMemoryPredicate(Set<I> ids) {
        final Predicate<Entity> idPredicate = new IdFilter(ids);
        return idPredicate;
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

    private Map<I, EntityRecord> lookup(Collection<I> ids, FieldMask fieldMask) {
        if (ids.isEmpty()) {
            return readAllRecords(fieldMask);
        }
        final Collection<Key> keys = toKeys(ids);
        final Collection<Entity> records = datastore.read(keys);
        final Map<I, EntityRecord> results = toRecordMap(records,
                                                         Predicates.<Entity>alwaysTrue(),
                                                         typeUrl,
                                                         fieldMask);
        return results;
    }

    private <T> Iterable<T> lookup(Iterable<I> ids,
                                   Function<Entity, T> transformer) {
        final Collection<Key> keys = toKeys(ids);
        final List<Entity> results = datastore.read(keys);
        final Collection<Entity> filteredResults = filter(results, activeEntity());
        final Collection<T> records = transform(filteredResults, transformer);
        return unmodifiableCollection(records);
    }

    private Map<I, EntityRecord> queryAll(TypeUrl typeUrl,
                                          StructuredQuery<Entity> query,
                                          FieldMask fieldMask) {
        return queryAll(typeUrl, query, fieldMask, activeEntity());
    }

    private Map<I, EntityRecord> queryAll(TypeUrl typeUrl,
                                          StructuredQuery<Entity> query,
                                          FieldMask fieldMask,
                                          Predicate<Entity> resultFilter) {
        final List<Entity> results = datastore.read(query);
        return toRecordMap(results, resultFilter, typeUrl, fieldMask);
    }

    private Map<I, EntityRecord> toRecordMap(Iterable<Entity> queryResults,
                                             Predicate<Entity> filter,
                                             TypeUrl typeUrl,
                                             FieldMask fieldMask) {
        final ImmutableMap.Builder<I, EntityRecord> records = new ImmutableMap.Builder<>();
        for (Entity entity : queryResults) {
            if (!filter.apply(entity)) {
                continue;
            }
            final IdRecordPair<I> recordPair = getRecordFromEntity(entity);
            EntityRecord record = recordPair.getRecord();

            if (!isDefault(fieldMask)) {
                Message state = unpack(record.getState());
                state = applyMask(fieldMask, state, typeUrl);
                record = EntityRecord.newBuilder(record)
                                     .setState(AnyPacker.pack(state))
                                     .build();
            }
            records.put(recordPair.getId(), record);
        }

        return records.build();
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

    private Kind kindFrom(TypeUrl typeUrl) {
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

    IdRecordPair<I> getRecordFromEntity(Entity entity) {
        // Retrieve ID
        final I id = unpackKey(entity);
        checkState(id != null, ID_CONVERSION_ERROR_MESSAGE);

        // Retrieve record
        final EntityRecord record = Entities.entityToMessage(entity, RECORD_TYPE_URL);
        return new IdRecordPair<>(id, record);
    }

    StructuredQuery<Entity> buildAllQuery(TypeUrl typeUrl) {
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
         * @param columnTypeRegistry the registry of the Entity
         *                           {@link io.spine.server.entity.storage.Column Columns} types
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

    /**
     * A tuple containing generic record identifier and corresponding {@link EntityRecord}.
     *
     * @param <I> type of the {@link io.spine.server.entity.Entity entity} ID.
     */
    protected static class IdRecordPair<I> {

        private final I id;
        private final EntityRecord record;

        protected IdRecordPair(I id, EntityRecord record) {
            this.id = id;
            this.record = record;
        }

        protected I getId() {
            return id;
        }

        protected EntityRecord getRecord() {
            return record;
        }
    }

    private class IdFilter implements Predicate<Entity> {

        private final Set<?> acceptedIds;

        private IdFilter(Set<?> acceptedIds) {
            this.acceptedIds = acceptedIds;
        }

        @Override
        public boolean apply(@Nullable Entity input) {
            if (input == null) {
                return false;
            }
            if (acceptedIds.isEmpty()) {
                return true;
            }
            final Object id = unpackKey(input);
            final boolean result = acceptedIds.contains(id);
            return result;
        }
    }
}