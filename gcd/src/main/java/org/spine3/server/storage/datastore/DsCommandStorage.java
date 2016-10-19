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
import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.datastore.newapi.DatastoreWrapper;
import org.spine3.server.storage.datastore.newapi.Entities;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.datastore.newapi.Entities.messageToEntity;
import static org.spine3.validate.Validate.checkNotDefault;

/**
 * Storage for command records based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsCommandStorage extends CommandStorage {

    private static final String TIMESTAMP_PROPERTY_NAME = "timestamp";
    private static final String TIMESTAMP_NANOS_PROPERTY_NAME = "timestamp_nanos";

    private final DatastoreWrapper datastore;

    private static final TypeUrl TYPE_URL = TypeUrl.of(CommandStorageRecord.getDescriptor());

    /* package */ static CommandStorage newInstance(DatastoreWrapper datastore, boolean multitenant) {
        return new DsCommandStorage(datastore, multitenant);
    }

    private DsCommandStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
    }

    @Override
    protected Iterator<CommandStorageRecord> read(CommandStatus status) {
        throw unsupported();
//        final String statusPropertyValue = CommandStorageRecord.getDescriptor()
//                .getFields()
//                .get(CommandStorageRecord.STATUS_FIELD_NUMBER - 1)
//                .getName();
//        final PropertyReference statusPropertyRef = PropertyReference.newBuilder()
//                .setName(statusPropertyValue)
//                .build();
//        final Value statusVal = Value.newBuilder()
//                .setStringValue(status.toString())
//                .build();
//        final PropertyFilter propertyFilter = PropertyFilter.newBuilder()
//                .setProperty(statusPropertyRef)
//                .setOp(PropertyFilter.Operator.EQUAL)
//                .setValue(statusVal)
//                .build();
//        final Filter statusFilter = Filter.newBuilder()
//                .setPropertyFilter(propertyFilter)
//                .build();
//        final KindExpression kind = KindExpression.newBuilder()
//                .setName(typeUrl.value())
//                .build();
//        final Query.Builder query = Query.newBuilder()
//                .setFilter(statusFilter)
//                .addKind(kind);
//        final List<EntityResult> queryResult = datastore.runQuery(query);
//        final Collection<CommandStorageRecord> records = Collections2.transform(
//                queryResult,
//                new Function<EntityResult, CommandStorageRecord>() {
//                    @Nullable
//                    @Override
//                    public CommandStorageRecord apply(@Nullable EntityResult input) {
//                        if (input == null) {
//                            return null;
//                        }
//                        return entityToMessage(input, typeUrl.value());
//                    }
//                });
//
//        return records.iterator();
    }

    private static RuntimeException unsupported() {
        throw new UnsupportedOperationException("By status read is not supported for Google Cloud Datastore implementation");
    }

    @Override
    public void setOkStatus(CommandId commandId) {
        checkNotNull(commandId);

        final CommandStorageRecord updatedRecord = read(commandId)
                .toBuilder()
                .setStatus(CommandStatus.OK)
                .build();
        write(commandId, updatedRecord);
    }

    @Override
    public void updateStatus(CommandId commandId, Error error) {
        checkNotNull(commandId);
        checkNotNull(error);

        final CommandStorageRecord updatedRecord = read(commandId)
                .toBuilder()
                .setStatus(CommandStatus.ERROR)
                .setError(error)
                .build();
        write(commandId, updatedRecord);
    }

    @Override
    public void updateStatus(CommandId commandId, Failure failure) {
        checkNotNull(commandId);
        checkNotNull(failure);

        final CommandStorageRecord updatedRecord = read(commandId)
                .toBuilder()
                .setStatus(CommandStatus.FAILURE)
                .setFailure(failure)
                .build();
        write(commandId, updatedRecord);
    }

    @Override
    public CommandStorageRecord read(CommandId commandId) {
        checkNotClosed();
        checkNotDefault(commandId);

        final String idString = idToString(commandId);
        final Key key = datastore.getKeyFactory(TYPE_URL.getTypeName()).newKey(idString);
        final Entity entity = datastore.read(key);

        if (entity == null) {
            return CommandStorageRecord.getDefaultInstance();
        }
        // TODO:18-10-16:dmytro.dashenkov: Add timestamp properties.
        final CommandStorageRecord record = Entities.entityToMessage(entity, TYPE_URL);
        return record;

//        final Key.Builder key = createKey(idString);
//        final LookupRequest request = LookupRequest.newBuilder().addKeys(key).build();
//
//        final LookupResponse response = datastore.lookup(request);
//
//        if (response == null || response.getFoundCount() == 0) {
//            return CommandStorageRecord.getDefaultInstance();
//        }
//
//        final EntityResult entity = response.getFound(0);
//        final CommandStorageRecord result = entityToMessage(entity, typeUrl.value());
//        return result;
    }

    @Override
    public void write(CommandId commandId, CommandStorageRecord record) {
        checkNotClosed();
        checkNotDefault(commandId);
        checkNotDefault(record);

        final String idString = idToString(commandId);

        final Key key = createKey(idString);

        Entity entity = messageToEntity(record, key);
        entity = Entity.builder(entity)
                .set(TIMESTAMP_PROPERTY_NAME, record.getTimestamp().getSeconds())
                .set(TIMESTAMP_NANOS_PROPERTY_NAME, record.getTimestamp().getNanos())
                .build();
        datastore.createOrUpdate(entity);
    }

    private Key createKey(String idString) {
        return datastore.getKeyFactory(TYPE_URL.getSimpleName()).newKey(idString);
    }
}
