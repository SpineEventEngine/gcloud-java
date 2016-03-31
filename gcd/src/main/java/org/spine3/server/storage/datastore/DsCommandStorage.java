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

import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.type.TypeName;

import static com.google.api.services.datastore.DatastoreV1.*;
import static com.google.api.services.datastore.client.DatastoreHelper.*;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.datastore.DatastoreWrapper.entityToMessage;
import static org.spine3.server.storage.datastore.DatastoreWrapper.makeTimestampProperty;
import static org.spine3.server.storage.datastore.DatastoreWrapper.messageToEntity;

/**
 * Storage for command records based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsCommandStorage extends CommandStorage {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String COMMAND_ID_PROPERTY_NAME = "commandId";

    private static final String KIND = CommandStorageRecord.class.getName();

    private final DatastoreWrapper datastore;

    private final TypeName typeName;

    protected static CommandStorage newInstance(DatastoreWrapper datastore) {
        return new DsCommandStorage(datastore);
    }

    private DsCommandStorage(DatastoreWrapper datastore) {
        this.datastore = datastore;
        typeName = TypeName.of(CommandStorageRecord.getDescriptor());
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
                .setStatus(CommandStatus.FAILURE)
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
        final String idString = idToString(commandId);
        final Key.Builder key = createKey(idString);
        final LookupRequest request = LookupRequest.newBuilder().addKey(key).build();

        final LookupResponse response = datastore.lookup(request);

        if (response == null || response.getFoundCount() == 0) {
            return CommandStorageRecord.getDefaultInstance();
        }

        final EntityResult entity = response.getFound(0);
        final CommandStorageRecord result = entityToMessage(entity, typeName.toTypeUrl());
        return result;
    }

    @Override
    public void write(CommandId commandId, CommandStorageRecord record) {
        final Value.Builder id = makeValue(record.getCommandId());
        final Property.Builder idProperty = makeProperty(COMMAND_ID_PROPERTY_NAME, id);
        final Entity.Builder entity = messageToEntity(record, makeKey(KIND));
        entity.addProperty(idProperty);
        entity.addProperty(makeTimestampProperty(record.getTimestamp()));

        final Mutation.Builder mutation = Mutation.newBuilder().addInsertAutoId(entity);
        datastore.commit(mutation);
    }

    private Key.Builder createKey(String idString) {
        return makeKey(typeName.nameOnly(), idString);
    }
}
