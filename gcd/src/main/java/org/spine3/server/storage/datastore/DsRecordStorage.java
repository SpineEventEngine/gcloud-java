/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.datastore.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import javafx.util.Pair;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.entity.FieldMasks;
import org.spine3.server.reflect.Classes;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.datastore.newapi.DatastoreWrapper;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static org.spine3.base.Identifiers.idToString;

/**
 * {@link RecordStorage} implementation based on Google App Engine Datastore.
 *
 * @author Alexander Litus
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsRecordStorage<I> extends RecordStorage<I> {

    private final DatastoreWrapper datastore;
    private final TypeUrl typeUrl;
    private final TypeUrl entityStorageRecordTypeUrl = TypeUrl.of(EntityStorageRecord.getDescriptor());

    private static final int ID_GENERIC_TYPE_NUMBER = 0;

    /* package */
    static <I> DsRecordStorage<I> newInstance(Descriptor descriptor, DatastoreWrapper datastore, boolean multitenant) {
        return new DsRecordStorage<>(descriptor, datastore, multitenant);
    }

    private final Function<EntityResult, EntityStorageRecord> recordFromEntity = new Function<EntityResult, EntityStorageRecord>() {
        @Nullable
        @Override
        public EntityStorageRecord apply(@Nullable EntityResult input) {
            if (input == null) {
                return null;
            }


            final Message state = entityToMessage(input, typeUrl.value());
            final Any wrappedState = AnyPacker.pack(state);

            @SuppressWarnings("NumericCastThatLosesPrecision")
            final EntityStorageRecord record = EntityStorageRecord.newBuilder()
                    .setState(wrappedState)
                    .setVersion((int) input.getVersion())
                    .build();
            return record;
        }
    };

    /**
     * Creates a new storage instance.
     *
     * @param descriptor the descriptor of the type of messages to save to the storage.
     * @param datastore  the datastore implementation to use.
     */
    private DsRecordStorage(Descriptor descriptor, DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.typeUrl = TypeUrl.of(descriptor);
        this.datastore = datastore;
    }

    @Nullable
    @Override
    protected EntityStorageRecord readRecord(I id) {
        final String idString = idToString(id);
        final Key.Builder key = createKey(idString);
        final LookupRequest request = LookupRequest.newBuilder()
                .addKeys(key)
                .build();

        final LookupResponse response = datastore.lookup(request);

        if (response == null || response.getFoundCount() == 0) {
            return EntityStorageRecord.getDefaultInstance();
        }

        final EntityResult entity = response.getFound(0);

        final EntityStorageRecord result = entityToMessage(entity, entityStorageRecordTypeUrl.value());
        return result;
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<I> ids) {
        return lookup(ids, recordFromEntity);
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<I> ids, final FieldMask fieldMask) {
        final Function<EntityResult, EntityStorageRecord> transformer = new Function<EntityResult, EntityStorageRecord>() {
            @Nullable
            @Override
            public EntityStorageRecord apply(@Nullable EntityResult input) {
                if (input == null) {
                    return null;
                }

                final Message state = entityToMessage(input, typeUrl.value());
                final Message maskedState = FieldMasks.applyMask(fieldMask, state, typeUrl);
                final Any wrappedState = AnyPacker.pack(maskedState);

                @SuppressWarnings("NumericCastThatLosesPrecision")
                final EntityStorageRecord record = EntityStorageRecord.newBuilder()
                        .setState(wrappedState)
                        .setVersion((int) input.getVersion())
                        .build();
                return record;
            }
        };

        return lookup(ids, transformer);
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllRecords() {
        throw unsupported();
//        final Function<EntityResult, Pair<I, EntityStorageRecord>> mapper
//                = new Function<EntityResult, Pair<I, EntityStorageRecord>>() {
//            @Nullable
//            @Override
//            public Pair<I, EntityStorageRecord> apply(@Nullable EntityResult input) {
//                if (input == null) {
//                    return null;
//                }
//
//                final Any packedId = Any.parseFrom(input.getEntity().)
//                final EntityStorageRecord record = recordFromEntity.apply(input);
//
//                return new Pair<>(null, record);
//            }
//        };
//
//        return queryAll(mapper);
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllRecords(FieldMask fieldMask) {
        throw unsupported();
    }

    private Iterable<EntityStorageRecord> lookup(
            Iterable<I> ids,
            Function<EntityResult, EntityStorageRecord> transformer) {

        final Collection<Key> keys = new LinkedList<>();
        for (I id : ids) {
            final String stringKey = idToString(id);
            final Key key = createKey(stringKey).build();
            keys.add(key);
        }

        final LookupRequest request = LookupRequest.newBuilder()
                .addAllKeys(keys)
                .build();

        final LookupResponse response = datastore.lookup(request);
        final Collection<EntityResult> results = response.getFoundList();
        final Collection<EntityStorageRecord> records = Collections2.transform(results, transformer);

        return Collections.unmodifiableCollection(records);
    }

    private Map<I, EntityStorageRecord> queryAll(Function<EntityResult, Pair<I, EntityStorageRecord>> transformer) {
        final KindExpression kind = KindExpression.newBuilder()
                .setName(typeUrl.getSimpleName())
                .build();
        final Query.Builder query = Query.newBuilder()
                .addKind(kind);
        final List<EntityResult> results = datastore.runQuery(query);

        final ImmutableMap.Builder<I, EntityStorageRecord> records = new ImmutableMap.Builder<>();
        for (EntityResult entity : results) {
            final Pair<I, EntityStorageRecord> recordPair = transformer.apply(entity);
            checkNotNull(recordPair, "Datastore may not contain null records.");
            records.put(recordPair.getKey(), recordPair.getValue());
        }

        return records.build();
    }

    @Override
    protected void writeRecord(I id, EntityStorageRecord entityStorageRecord) {
        checkNotNull(id, "ID is null.");
        checkNotNull(entityStorageRecord, "Message is null.");

        final String idString = idToString(id);
        final Key.Builder key = createKey(idString);
        final Entity.Builder entity = messageToEntity(entityStorageRecord, key);
        WriteOperations.createOrUpdate(entity.build(), datastore);
    }

    private Key.Builder createKey(String idString) {
        return makeKey(typeUrl.getSimpleName(), idString);
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private I idFromString(String stringId) {
        final Class<I> idClass = getIdClass();
        final I id;
        if (idClass.isPrimitive()) { // Numeric ID
            final String typeName = idClass.getSimpleName();
            try {
                final Method parser = idClass.getDeclaredMethod("parse" + typeName, String.class);
                id = (I) parser.invoke(null, stringId);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                // TODO:17-10-16:dmytro.dashenkov: Log.
                // Never happen
                return null;
            }
        } else if (idClass.equals(String.class)) { // String ID
            id = (I) stringId;
        } else { // Proto Message ID
            final Any packed;
            try {
                packed = Any.parseFrom(stringId.getBytes());
                id = AnyPacker.unpack(packed);
            } catch (InvalidProtocolBufferException e) {
                // TODO:17-10-16:dmytro.dashenkov: Log.
                return null;
            }
        }

        return id;
    }

    private static RuntimeException unsupported() {
        return new UnsupportedOperationException(
                "Read-all operations are not supported on Google Cloud Datastore implementation.");
    }

    private Class<I> getIdClass() {
        return Classes.getGenericParameterType(this.getClass(), ID_GENERIC_TYPE_NUMBER);
    }
}
