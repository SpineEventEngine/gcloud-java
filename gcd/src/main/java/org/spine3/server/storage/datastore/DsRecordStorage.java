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
import org.spine3.server.entity.storage.Column;
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
import static org.spine3.server.storage.datastore.DsProperties.activedEntityPredicate;
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
    protected static final TypeUrl RECORD_TYPE_URL = TypeUrl.of(EntityRecord.class);
    protected static final String ID_CONVERSION_ERROR_MESSAGE = "Entity had ID of an invalid type; could not " +
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
        return queryAll(typeUrl, fieldMask);
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
        final Collection<Entity> filteredResults = Collections2.filter(results, activedEntityPredicate());
        final Collection<EntityRecord> records = Collections2.transform(filteredResults, transformer);
        return Collections.unmodifiableCollection(records);
    }

    protected Map<I, EntityRecord> queryAll(TypeUrl typeUrl,
                                            FieldMask fieldMask) {
        final EntityQuery query = buildAllQuery(typeUrl);

        final List<Entity> results = datastore.read(query);

        final Predicate<Entity> archivedAndDeletedFilter = activedEntityPredicate();

        final ImmutableMap.Builder<I, EntityRecord> records = new ImmutableMap.Builder<>();
        for (Entity entity : results) {
            if (!archivedAndDeletedFilter.apply(entity)) {
                continue;
            }
            final IdRecordPair<I> recordPair = getRecordFromEntity(entity, typeUrl);
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

    protected EntityQuery buildAllQuery(TypeUrl typeUrl) {
        final String entityKind = kindFrom(typeUrl).getValue();
        final EntityQuery query = Query.newEntityQueryBuilder()
                                       .setKind(entityKind)
                                       .build();
        return query;
    }

    protected Entity entityRecordToEntity(I id, EntityRecordWithStorageFields record) {
        final EntityRecord entityRecord = record.getRecord();
        final Key key = keyFor(datastore,
                               kindFrom(entityRecord),
                               ofEntityId(id));
        final Entity incompleteEntity = Entities.messageToEntity(entityRecord, key);

        if (record.hasStorageFields()) {
            final Map<String, Column.MemoizedValue<?>> storageFields = record.getStorageFields();
            for (Map.Entry<String, Column.MemoizedValue<?>> field : storageFields.entrySet()) {
                final Column<?> storageColumn = field.getValue()
                                                     .getSourceColumn();
                final Class<?> fieldType = storageColumn.getType();
//                final DatastoreColumnType<?, ?> columnType =
            }
        }

        final Entity.Builder entity = Entity.newBuilder(incompleteEntity);
        entity.set(VERSION_KEY, entityRecord.getVersion()
                                            .getNumber());
        final Entity completeEntity = entity.build();
        return completeEntity;
    }

    @Override
    protected void writeRecord(I id, EntityRecordWithStorageFields entityStorageRecord) {
        checkNotNull(id, "ID is null.");
        checkNotNull(entityStorageRecord, "Message is null.");

        final Entity entity = entityRecordToEntity(id, entityStorageRecord);
        datastore.createOrUpdate(entity);
    }

    @Override
    protected void writeRecords(Map<I, EntityRecordWithStorageFields> records) {
        checkNotNull(records);

        final Collection<Entity> entitiesToWrite = new ArrayList<>(records.size());
        for (Map.Entry<I, EntityRecordWithStorageFields> record : records.entrySet()) {
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

    protected I unpackKey(Key key, @SuppressWarnings("unused") TypeUrl stateType) {
        final I id = IdTransformer.idFromString(key.getName(), null);
        return id;
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

    protected IdRecordPair<I> getRecordFromEntity(Entity entity, TypeUrl stateType) {
        // Retrieve ID
        final I id = unpackKey(entity.getKey(), stateType);
        checkState(id != null, ID_CONVERSION_ERROR_MESSAGE);

        // Retrieve record
        EntityRecord record = Entities.entityToMessage(entity, RECORD_TYPE_URL);
        final Any packedState = record.getState();
        Message state = AnyPacker.unpack(packedState);
        record = EntityRecord.newBuilder(record)
                             .setState(AnyPacker.pack(state))
                             .build();
        return new IdRecordPair<>(id, record);
    }

    protected Kind getKind() {
        return kindFrom(typeUrl);
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
}
