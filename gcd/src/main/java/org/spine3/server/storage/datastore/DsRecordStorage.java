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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.FieldMasks;
import org.spine3.server.entity.storage.ColumnRecords;
import org.spine3.server.entity.storage.ColumnTypeRegistry;
import org.spine3.server.entity.storage.CompositeQueryParameter;
import org.spine3.server.entity.storage.EntityQuery;
import org.spine3.server.entity.storage.EntityRecordWithColumns;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.datastore.type.DatastoreColumnType;
import org.spine3.string.Stringifiers;
import org.spine3.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.api.client.util.Maps.newHashMap;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.spine3.server.storage.datastore.DsIdentifiers.keyFor;
import static org.spine3.server.storage.datastore.DsIdentifiers.ofEntityId;
import static org.spine3.server.storage.datastore.Entities.activeEntity;
import static org.spine3.validate.Validate.isDefault;

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
    private final ColumnHandler columnHandler;
    private final Class<I> idClass;

    private static final TypeUrl RECORD_TYPE_URL = TypeUrl.of(EntityRecord.class);
    private static final String ID_CONVERSION_ERROR_MESSAGE =
            "Entity had ID of an invalid type; could not parse ID from String. " +
            "Note: custom conversion is not supported. See org.spine3.base.Identifiers#idToString.";

    private static final String KEY_PROPERTY = "__key__";
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
        this.columnHandler = new ColumnHandler(this.columnTypeRegistry);
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
                final Message state = AnyPacker.unpack(readRecord.getState());
                final TypeUrl typeUrl = TypeUrl.from(state.getDescriptorForType());
                final Message maskedState = FieldMasks.applyMask(fieldMask, state, typeUrl);
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

        boolean idsHandled = false;
        final Set<I> ids = entityQuery.getIds();
        if (ids.isEmpty()) {
            idsHandled = true;
        } else if (ids.size() == 1) {
//            idsHandled = true;
//            final Object singleId = ids.iterator().next();
//            final Filter idFilter = buildSingleIdFilter(singleId);
//            filter = and(filter, idFilter);
        }

        final Collection<StructuredQuery<Entity>> queries = Collections2.transform(
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

        final Predicate<Entity> inMemFilter = buildMemoryPredicate(!idsHandled, ids);
        final Map<I, EntityRecord> result = newHashMap();
        for (StructuredQuery<Entity> query : queries) {
            final Map<I, EntityRecord> records = queryAll(typeUrl, query, fieldMask, inMemFilter);
            result.putAll(records);
        }
        return result;
    }

    private Predicate<Entity> buildMemoryPredicate(boolean handleIds,
                                                   Set<I> ids) {
        final Predicate<Entity> idPredicate = handleIds
                                              ? new IdFilter(ids)
                                              : Predicates.<Entity>alwaysTrue();
        return idPredicate;
    }

    private Collection<Filter> buildColumnFilters(
            Iterable<CompositeQueryParameter> compositeParameters) {
        final Collection<CompositeQueryParameter> params = newArrayList(compositeParameters);
        final Collection<Filter> predicate = DsQueries.fromParams(params, columnHandler);
        return predicate;
    }

    private Filter buildSingleIdFilter(Object id) {
        final Key key = keyFor(datastore,
                               kindFrom(typeUrl),
                               ofEntityId(id));
        final PropertyFilter filter = eq(KEY_PROPERTY, key);
        return filter;
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

    private Iterable<EntityRecord> lookup(
            Iterable<I> ids,
            Function<Entity, EntityRecord> transformer) {
        final Collection<Key> keys = new LinkedList<>();
        for (I id : ids) {
            final Key key = keyFor(datastore,
                                   kindFrom(typeUrl),
                                   ofEntityId(id));
            keys.add(key);
        }
        final List<Entity> results = datastore.read(keys);
        final Collection<Entity> filteredResults = filter(results, activeEntity());
        final Collection<EntityRecord> records = transform(filteredResults, transformer);
        return Collections.unmodifiableCollection(records);
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

        final ImmutableMap.Builder<I, EntityRecord> records = new ImmutableMap.Builder<>();
        for (Entity entity : results) {
            if (!resultFilter.apply(entity)) {
                continue;
            }
            final IdRecordPair<I> recordPair = getRecordFromEntity(entity);
            EntityRecord record = recordPair.getRecord();

            if (!isDefault(fieldMask)) {
                Message state = AnyPacker.unpack(record.getState());
                state = FieldMasks.applyMask(fieldMask, state, typeUrl);
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
        final Message state = AnyPacker.unpack(packedState);
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
         *                    {@link org.spine3.server.storage.Storage#isMultitenant multitenant}
         *                    or not
         */
        public Builder<I> setMultitenant(boolean multitenant) {
            this.multitenant = multitenant;
            return this;
        }

        /**
         * @param columnTypeRegistry the registry of the Entity
         *                           {@link org.spine3.server.entity.storage.Column Columns} types
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
     * @param <I> type of the {@link org.spine3.server.entity.Entity entity} ID.
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
            final Object id = unpackKey(input);
            final boolean result = acceptedIds.contains(id);
            return result;
        }
    }
}
