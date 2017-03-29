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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.FieldMasks;
import org.spine3.server.entity.LifecycleFlags;
import org.spine3.server.entity.storage.EntityRecordWithStorageFields;
import org.spine3.server.storage.RecordStorage;
import org.spine3.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.spine3.server.storage.datastore.DsIdentifiers.keyFor;
import static org.spine3.server.storage.datastore.DsIdentifiers.ofEntityId;
import static org.spine3.server.storage.datastore.DsProperties.activeEntityPredicate;
import static org.spine3.server.storage.datastore.Entities.getEntityStatus;
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
    private final Class<I> idClass;

    private static final String VERSION_KEY = "version";
    private static final String TYPE_URL_PROPERTY_NAME = "type_url";
    private static final TypeUrl RECORD_TYPE_URL = TypeUrl.of(EntityRecord.class);
    private static final String KIND = RECORD_TYPE_URL.value();
    private static final String ID_CONVERSION_ERROR_MESSAGE = "Entity had ID of an invalid type; could not " +
            "parse ID from String. " +
            "Note: custom conversion is not supported. " +
            "See org.spine3.base.Identifiers#idToString.";

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
    public DsRecordStorage(Descriptor descriptor, DatastoreWrapper datastore, boolean multitenant, Class<I> idClass) {
        super(multitenant);
        this.typeUrl = TypeUrl.from(descriptor);
        this.datastore = datastore;
        this.idClass = checkNotNull(idClass);
    }

    @Override
    public boolean delete(I id) {
        final Key key = keyFor(datastore, KIND, ofEntityId(id));
        datastore.delete(key);

        // Check presence
        final Entity record = datastore.read(key);
        return record == null;
    }

    @Nullable
    @Override
    protected Optional<EntityRecord> readRecord(I id) {
        final Key key = keyFor(datastore, KIND, ofEntityId(id));
        final Entity response = datastore.read(key);

        if (response == null) {
            return Optional.absent();
        }

        final EntityRecord record = Entities.entityToMessage(response, RECORD_TYPE_URL);
        final LifecycleFlags entityStatus = getEntityStatus(response);
        final EntityRecord result = isDefault(entityStatus) // Avoid inequality of written and read records
                                           ? record                // caused by empty {@code EntityStatus} object
                                           : EntityRecord.newBuilder(record)
                                                                .setLifecycleFlags(entityStatus)
                                                                .build();

        return Optional.of(result);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return lookup(ids, recordFromEntity);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<I> ids, final FieldMask fieldMask) {
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

                final LifecycleFlags entityStatus = getEntityStatus(input);
                final EntityRecord record = EntityRecord.newBuilder(readRecord)
                                                                      .setState(wrappedState)
                                                                      .setLifecycleFlags(entityStatus)
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
        final Function<Entity, IdRecordPair<I>> mapper = new Function<Entity, IdRecordPair<I>>() {
            @Nullable
            @Override
            public IdRecordPair<I> apply(@Nullable Entity input) {
                if (input == null) {
                    return null;
                }
                // Retrieve ID
                final I id = IdTransformer.idFromString(input.getKey()
                                                             .getName(), null);
                checkState(id != null, ID_CONVERSION_ERROR_MESSAGE);

                // Retrieve record
                EntityRecord record = Entities.entityToMessage(input, RECORD_TYPE_URL);
                final Any packedState = record.getState();
                Message state = AnyPacker.unpack(packedState);
                final TypeUrl typeUrl = TypeUrl.from(state.getDescriptorForType());
                state = FieldMasks.applyMask(fieldMask, state, typeUrl);

                record = EntityRecord.newBuilder(record)
                                     .setState(AnyPacker.pack(state))
                                     .build();
                return new IdRecordPair<>(id, record);
            }
        };

        return queryAll(mapper, null, FieldMask.getDefaultInstance());
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
     * @return the {@code TyprUrl} of the stored messages
     */
    protected TypeUrl getTypeUrl() {
        return typeUrl;
    }

    public Map<?, EntityRecord> readAllByType(final TypeUrl typeUrl, final FieldMask fieldMask) {
        final Function<Entity, IdRecordPair<I>> mapper = new Function<Entity, IdRecordPair<I>>() {
            @Nullable
            @Override
            public IdRecordPair<I> apply(@Nullable Entity input) {
                if (input == null) {
                    return null;
                }
                // Retrieve ID
                final I id = IdTransformer.idFromString(input.getKey()
                                                             .getName(), null);
                checkState(id != null, ID_CONVERSION_ERROR_MESSAGE);

                // Retrieve record
                EntityRecord record = Entities.entityToMessage(input, RECORD_TYPE_URL);
                final Any packedState = record.getState();
                Message state = AnyPacker.unpack(packedState);
                state = FieldMasks.applyMask(fieldMask, state, typeUrl);
                final LifecycleFlags entityStatus = getEntityStatus(input);
                record = EntityRecord.newBuilder(record)
                                            .setState(AnyPacker.pack(state))
                                            .setLifecycleFlags(entityStatus)
                                            .build();
                return new IdRecordPair<>(id, record);
            }
        };

        return queryAll(mapper, typeUrl, FieldMask.getDefaultInstance());
    }

    public Map<?, EntityRecord> readAllByType(final TypeUrl typeUrl) {
        final Function<Entity, IdRecordPair<I>> mapper = new Function<Entity, IdRecordPair<I>>() {
            @Nullable
            @Override
            public IdRecordPair<I> apply(@Nullable Entity input) {
                if (input == null) {
                    return null;
                }
                // Retrieve ID
                final I id = IdTransformer.idFromString(input.getKey()
                                                             .getName(), null);
                checkState(id != null, ID_CONVERSION_ERROR_MESSAGE);

                // Retrieve record
                EntityRecord record = Entities.entityToMessage(input, RECORD_TYPE_URL);
                final Any packedState = record.getState();
                Message state = AnyPacker.unpack(packedState);
                final LifecycleFlags entityStatus = getEntityStatus(input);
                record = EntityRecord.newBuilder(record)
                                            .setState(AnyPacker.pack(state))
                                            .setLifecycleFlags(entityStatus)
                                            .build();

                return new IdRecordPair<>(id, record);
            }
        };

        return queryAll(mapper, typeUrl, FieldMask.getDefaultInstance());
    }

    private Iterable<EntityRecord> lookup(
            Iterable<I> ids,
            Function<Entity, EntityRecord> transformer) {

        final Collection<Key> keys = new LinkedList<>();
        for (I id : ids) {
            final Key key = keyFor(datastore, KIND, ofEntityId(id));
            keys.add(key);
        }

        final List<Entity> results = datastore.read(keys);
        final Collection<Entity> filteredResults = Collections2.filter(results, activeEntityPredicate());
        final Collection<EntityRecord> records = Collections2.transform(filteredResults, transformer);
        return Collections.unmodifiableCollection(records);
    }

    private Map<I, EntityRecord> queryAll(Function<Entity, IdRecordPair<I>> transformer,
                                                 @Nullable TypeUrl typeForFilter,
                                                 FieldMask fieldMask) {

        EntityQuery.Builder builder = Query.newEntityQueryBuilder()
                                           .setKind(KIND);
        if (typeForFilter != null) {
            final PropertyFilter typeFilter = PropertyFilter.eq(TYPE_URL_PROPERTY_NAME, typeForFilter.value());
            builder = builder.setFilter(typeFilter);
        }
        final EntityQuery query = builder.build();

        final List<Entity> results = datastore.read(query);

        final Predicate<Entity> archivedAndDeletedFilter = activeEntityPredicate();

        final ImmutableMap.Builder<I, EntityRecord> records = new ImmutableMap.Builder<>();
        for (Entity entity : results) {
            if (!archivedAndDeletedFilter.apply(entity)) {
                continue;
            }
            final IdRecordPair<I> recordPair = transformer.apply(entity);
            checkNotNull(recordPair, "Datastore may not contain null records.");
            final EntityRecord fullRecord = recordPair.getRecord();
            final Message fullState = AnyPacker.unpack(fullRecord.getState());
            final Message maskedState = FieldMasks.applyMask(fieldMask, fullState, typeUrl);
            final EntityRecord maskedRecord = EntityRecord.newBuilder(fullRecord)
                                                          .setState(AnyPacker.pack(maskedState))
                                                          .build();
            records.put(recordPair.getId(), maskedRecord);
        }

        return records.build();
    }

    @Override
    protected void writeRecord(I id, EntityRecordWithStorageFields entityStorageRecord) {
        checkNotNull(id, "ID is null.");
        checkNotNull(entityStorageRecord, "Message is null.");

        final EntityRecord record = entityStorageRecord.getRecord();
        final String valueTypeUrl = record.getState()
                                          .getTypeUrl();

        final Key key = keyFor(datastore, KIND, ofEntityId(id));
        final Entity incompleteEntity = Entities.messageToEntity(record, key);
        final Entity.Builder entity = Entity.newBuilder(incompleteEntity);
        entity.set(VERSION_KEY, record.getVersion()
                                      .getNumber());
        entity.set(TYPE_URL_PROPERTY_NAME, valueTypeUrl);
        datastore.createOrUpdate(entity.build());
    }

    @Override
    protected void writeRecords(Map<I, EntityRecordWithStorageFields> records) {
        checkNotNull(records);

        final Collection<Entity> entitiesToWrite = new ArrayList<>(records.size());
        for (Map.Entry<I, EntityRecordWithStorageFields> record : records.entrySet()) {
            final EntityRecord entityStorageRecord = record.getValue()
                                                           .getRecord();
            final String valueTypeUrl = entityStorageRecord.getState()
                                                           .getTypeUrl();
            final Key key = keyFor(
                    datastore,
                    KIND,
                    ofEntityId(record.getKey()));
            final Entity incompleteEntity = Entities.messageToEntity(entityStorageRecord, key);
            final Entity.Builder entity = Entity.newBuilder(incompleteEntity);
            entity.set(VERSION_KEY, entityStorageRecord.getVersion().getNumber());
            entity.set(TYPE_URL_PROPERTY_NAME, valueTypeUrl);
            entitiesToWrite.add(entity.build());
        }
        datastore.createOrUpdate(entitiesToWrite);
    }

    @Override
    public Iterator<I> index() {
        checkNotClosed();
        final StructuredQuery.Filter filter = PropertyFilter.eq(TYPE_URL_PROPERTY_NAME, typeUrl.getTypeName());
        return Indexes.indexIterator(datastore, KIND, idClass, filter);
    }

    /**
     * A tuple containing generic record identifier and corresponding {@link EntityRecord}.
     *
     * @param <I> type of the {@link org.spine3.server.entity.Entity entity} ID.
     */
    private static class IdRecordPair<I> {

        private final I id;
        private final EntityRecord record;

        private IdRecordPair(I id, EntityRecord record) {
            this.id = id;
            this.record = record;
        }

        private I getId() {
            return id;
        }

        private EntityRecord getRecord() {
            return record;
        }
    }
}
