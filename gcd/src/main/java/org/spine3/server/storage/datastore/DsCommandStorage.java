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
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.CommandStorageRecord;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;

import static com.google.cloud.datastore.StructuredQuery.*;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.datastore.DatastoreProperties.TIMESTAMP_NANOS_PROPERTY_NAME;
import static org.spine3.server.storage.datastore.DatastoreProperties.TIMESTAMP_PROPERTY_NAME;
import static org.spine3.server.storage.datastore.Entities.messageToEntity;
import static org.spine3.validate.Validate.checkNotDefault;

/**
 * Storage for command records based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsCommandStorage extends CommandStorage {

    private static final TypeUrl TYPE_URL = TypeUrl.of(CommandStorageRecord.getDescriptor());
    private static final String COMMAND_STATUS_PRORPERTY_NAME = "command_status";

    private final DatastoreWrapper datastore;

    private static final Function<Entity, CommandStorageRecord> RECORD_MAPPER = new Function<Entity, CommandStorageRecord>() {
        @Nullable
        @Override
        public CommandStorageRecord apply(@Nullable Entity input) {
            checkNotNull(input);
            final CommandStorageRecord record = Entities.entityToMessage(input, TYPE_URL);
            // TODO:18-10-16:dmytro.dashenkov: Add timestamp properties.
            return record;
        }
    };

    /* package */ static CommandStorage newInstance(DatastoreWrapper datastore, boolean multitenant) {
        return new DsCommandStorage(datastore, multitenant);
    }

    private DsCommandStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
    }

    @Override
    protected Iterator<CommandStorageRecord> read(CommandStatus status) {
        final Filter filter = PropertyFilter.eq(COMMAND_STATUS_PRORPERTY_NAME, status.ordinal());
        final Query query = Query.entityQueryBuilder()
                .kind(TYPE_URL.getSimpleName())
                .filter(filter)
                .build();
        final Collection<Entity> entities = datastore.read(query);
        final Collection<CommandStorageRecord> records = Collections2.transform(entities, RECORD_MAPPER);
        return records.iterator();
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
        final Key key = createKey(idString);
        final Entity entity = datastore.read(key);

        if (entity == null) {
            return CommandStorageRecord.getDefaultInstance();
        }
        final CommandStorageRecord record = RECORD_MAPPER.apply(entity);
        return record;
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
                .set(COMMAND_STATUS_PRORPERTY_NAME, record.getStatus().ordinal())
                .build();
        datastore.createOrUpdate(entity);
    }

    private Key createKey(String idString) {
        return datastore.getKeyFactory(TYPE_URL.getSimpleName()).newKey(idString);
    }
}
