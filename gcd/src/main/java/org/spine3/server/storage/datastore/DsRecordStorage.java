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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import javafx.util.Pair;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.entity.FieldMasks;
import org.spine3.server.reflect.Classes;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.RecordStorage;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.*;

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

    private static final String VERSION_KEY = "version";
    private static final int ID_GENERIC_TYPE_NUMBER = 0;
    private static final TypeUrl RECORD_TYPE_URL = TypeUrl.of(EntityStorageRecord.class);
    private static final Logger LOG = Logger.getLogger(DsRecordStorage.class.getCanonicalName());
    @SuppressWarnings("HardcodedLineSeparator")
    private static final String REFLECTIVE_ERROR_MESSAGE_PATTERN
            = "Reflective operation error trying to parse %s instance from string \"%s\"\n";
    @SuppressWarnings("HardcodedLineSeparator")
    private static final String PROTO_PARSING_ERROR_MESSAGE = "Could not parse an Any from byte array: \n";
    private static final String ID_CONVERTION_ERROR_MESSAGE
            = "Entity had ID of an invalid type; could not parse ID from String. Note: custom convection is not supported. See Identifiers#idToString.";
    private static final String WRONG_ID_TYPE_ERROR_MESSAGE
            = "Only String, numeric and Proto-message identifiers are allowed in GAE storage implementation.";
    private static final int STRING_BUILDER_INITIAL_CAPACITY = 128;
    private static final String TYPE_PREFIX = "TYPE::";
    private static final String SERIALIZED_MESSAGE_BYTES_DIVIDER = "-";
    private static final String SERIALIZED_MESSAGE_BYTES_POSTFIX = "::END";
    private static final String SERIALIZED_MESSAGE_DIVIDER = "::::";
    private static final String WRONG_OR_BROKEN_MESSAGE_ID = "Passed proto ID %s is wrong or broken.";

    /* package */
    static <I> DsRecordStorage<I> newInstance(Descriptor descriptor, DatastoreWrapper datastore, boolean multitenant) {
        return new DsRecordStorage<>(descriptor, datastore, multitenant);
    }

    private static final Function<Entity, EntityStorageRecord> recordFromEntity = new Function<Entity, EntityStorageRecord>() {
        @Nullable
        @Override
        public EntityStorageRecord apply(@Nullable Entity input) {
            if (input == null) {
                return null;
            }

            final EntityStorageRecord record = Entities.entityToMessage(input, RECORD_TYPE_URL);
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
        final Key key = createKey(id);

        final Entity response = datastore.read(key);

        if (response == null) {
            return EntityStorageRecord.getDefaultInstance();
        }

        return Entities.entityToMessage(response, RECORD_TYPE_URL);
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<I> ids) {
        return lookup(ids, recordFromEntity);
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<I> ids, final FieldMask fieldMask) {
        final Function<Entity, EntityStorageRecord> transformer = new Function<Entity, EntityStorageRecord>() {
            @Nullable
            @Override
            public EntityStorageRecord apply(@Nullable Entity input) {
                if (input == null) {
                    return null;
                }

                final EntityStorageRecord readRecord = Entities.entityToMessage(input, RECORD_TYPE_URL);
                final Message state = AnyPacker.unpack(readRecord.getState());
                final Message maskedState = FieldMasks.applyMask(fieldMask, state, typeUrl);
                final Any wrappedState = AnyPacker.pack(maskedState);

                final EntityStorageRecord record = EntityStorageRecord.newBuilder(readRecord)
                        .setState(wrappedState)
                        .build();
                return record;
            }
        };

        return lookup(ids, transformer);
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllRecords() {
        return readAllRecords(FieldMask.getDefaultInstance());
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllRecords(final FieldMask fieldMask) {
        final Function<Entity, Pair<I, EntityStorageRecord>> mapper
                = new Function<Entity, Pair<I, EntityStorageRecord>>() {
            @Nullable
            @Override
            public Pair<I, EntityStorageRecord> apply(@Nullable Entity input) {
                if (input == null) {
                    return null;
                }
                // Retrieve ID
                final I id = idFromString(input.key().name());
                checkState(id != null, ID_CONVERTION_ERROR_MESSAGE);

                // Retrieve record
                EntityStorageRecord record = Entities.entityToMessage(input, RECORD_TYPE_URL);
                final Any packedState = record.getState();
                Message state = AnyPacker.unpack(packedState);
                state = FieldMasks.applyMask(fieldMask, state, typeUrl);
                record = EntityStorageRecord.newBuilder(record)
                        .setState(AnyPacker.pack(state))
                        .build();

                return new Pair<>(id, record);
            }
        };

        return queryAll(mapper, FieldMask.getDefaultInstance());
    }

    private Iterable<EntityStorageRecord> lookup(
            Iterable<I> ids,
            Function<Entity, EntityStorageRecord> transformer) {

        final Collection<Key> keys = new LinkedList<>();
        for (I id : ids) {
            final Key key = createKey(id);
            keys.add(key);
        }

        final List<Entity> results = datastore.read(keys);
        final Collection<EntityStorageRecord> records = Collections2.transform(results, transformer);

        return Collections.unmodifiableCollection(records);
    }

    private Map<I, EntityStorageRecord> queryAll(Function<Entity, Pair<I, EntityStorageRecord>> transformer, FieldMask fieldMask) {
        final String sql = "SELECT * FROM " + RECORD_TYPE_URL.getSimpleName();
        final Query<?> query = Query.gqlQueryBuilder(sql).build();
        final List<Entity> results = datastore.read(query);

        final ImmutableMap.Builder<I, EntityStorageRecord> records = new ImmutableMap.Builder<>();
        for (Entity entity : results) {
            final Pair<I, EntityStorageRecord> recordPair = transformer.apply(entity);
            checkNotNull(recordPair, "Datastore may not contain null records.");
            final EntityStorageRecord fullRecord = recordPair.getValue();
            final Message fullState = AnyPacker.unpack(fullRecord.getState());
            final Message maskedState = FieldMasks.applyMask(fieldMask, fullState, typeUrl);
            final EntityStorageRecord maskedRecord = EntityStorageRecord.newBuilder(fullRecord)
                    .setState(AnyPacker.pack(maskedState))
                    .build();
            records.put(recordPair.getKey(), maskedRecord);
        }

        return records.build();
    }

    @Override
    protected void writeRecord(I id, EntityStorageRecord entityStorageRecord) {
        checkNotNull(id, "ID is null.");
        checkNotNull(entityStorageRecord, "Message is null.");

        final Key key = createKey(id);
        final Entity incompleteEntity = Entities.messageToEntity(entityStorageRecord, key);
        final Entity.Builder entity = Entity.builder(incompleteEntity);
        entity.set(VERSION_KEY, entityStorageRecord.getVersion());
        datastore.createOrUpdate(entity.build());
    }

    private Key createKey(I id) {
        final String idString;
        if (id instanceof String) { // String ID
            idString = (String) id;
        } else if (isNumber(id.getClass())) { // Numeric ID
            idString = id.toString();
        } else { // Proto-Message ID
            checkArgument(id instanceof Message, WRONG_ID_TYPE_ERROR_MESSAGE);

            final Message message = (Message) id;
            final StringBuilder idBuilder = new StringBuilder(STRING_BUILDER_INITIAL_CAPACITY);
            idBuilder
                    .append(TYPE_PREFIX)
                    .append(message.getDescriptorForType().getFullName())
                    .append("");
            final byte[] serializedMessage = message.toByteArray();
            for (byte b : serializedMessage) {
                idBuilder.append(b).append(SERIALIZED_MESSAGE_BYTES_DIVIDER);
            }
            idBuilder.append(SERIALIZED_MESSAGE_BYTES_POSTFIX);

            idString = idBuilder.toString();
        }
        return datastore.getKeyFactory(RECORD_TYPE_URL.getSimpleName()).newKey(idString);
    }

    private static boolean isNumber(Class<?> clss) {
        return clss.isPrimitive()
                || clss.isAssignableFrom(Integer.class)
                || clss.isAssignableFrom(Long.class);
    }

    private static Message protoIdFromString(String stringId) {
        checkArgument(stringId.startsWith(TYPE_PREFIX), String.format(WRONG_OR_BROKEN_MESSAGE_ID, stringId));
        checkArgument(stringId.endsWith(SERIALIZED_MESSAGE_BYTES_POSTFIX), String.format(WRONG_OR_BROKEN_MESSAGE_ID, stringId));

        final String bytesString = stringId.substring(
                stringId.indexOf(SERIALIZED_MESSAGE_DIVIDER),
                stringId.lastIndexOf(SERIALIZED_MESSAGE_BYTES_POSTFIX));
        final String[] separateStringBytes = bytesString.split(SERIALIZED_MESSAGE_BYTES_DIVIDER);
        final byte[] messageBytes = new byte[separateStringBytes.length];
        for (int i = 0; i < messageBytes.length; i++) {
            final byte oneByte = Byte.parseByte(separateStringBytes[i]);
            messageBytes[i] = oneByte;
        }

        final String typeName = stringId.substring(
                stringId.indexOf(TYPE_PREFIX),
                stringId.indexOf(SERIALIZED_MESSAGE_DIVIDER));

        final ByteString byteString = ByteString.copyFrom(messageBytes);
        final Any wrappedId = Any.newBuilder()
                .setValue(byteString)
                .setTypeUrl(typeName)
                .build();
        final Message id = AnyPacker.unpack(wrappedId);
        return id;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private I idFromString(String stringId) {
        final Class<I> idClass = getIdClass(stringId);
        final I id;
        if (isNumber(idClass)) { // Numeric ID
            final String typeName = idClass.getSimpleName();
            try {
                final Method parser = idClass.getDeclaredMethod("parse" + typeName, String.class);
                id = (I) parser.invoke(null, stringId);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                // Never happen
                LOG.warning(String.format(REFLECTIVE_ERROR_MESSAGE_PATTERN, typeName, stringId));
                return null;
            }
        } else if (idClass.isAssignableFrom(String.class)) { // String ID
            id = (I) stringId;
        } else { // Proto Message ID
            id = (I) protoIdFromString(stringId);
        }

        return id;
    }

    private Class<I> getIdClass(String stringId) {
        try {
            return Classes.getGenericParameterType(this.getClass(), ID_GENERIC_TYPE_NUMBER);
        } catch (ClassCastException e) { // If the class can't be defined we try to apply com.google.protobuf.Message
            return tryAllSupportedClasses(stringId);
        }
    }

    @SuppressWarnings("unchecked")
    private Class<I> tryAllSupportedClasses(String stringId) {
        if (stringId.startsWith(TYPE_PREFIX)) {
            return (Class<I>) Message.class;
        } else {
            try {
                Long.parseLong(stringId);
                return (Class<I>) Long.class;
            } catch (NumberFormatException ignored) {
                return (Class<I>) String.class;
            }
        }
    }
}
