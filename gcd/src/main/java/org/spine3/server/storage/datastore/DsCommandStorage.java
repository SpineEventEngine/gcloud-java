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
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.command.CommandRecord;
import org.spine3.server.command.CommandStorage;
import org.spine3.server.command.ProcessingStatus;
import org.spine3.type.TypeName;
import org.spine3.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;

import static com.google.cloud.datastore.StructuredQuery.Filter;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.EntityField.timestamp;
import static org.spine3.server.storage.EntityField.timestamp_nanos;
import static org.spine3.server.storage.datastore.DatastoreIdentifiers.of;
import static org.spine3.server.storage.datastore.Entities.messageToEntity;
import static org.spine3.validate.Validate.checkNotDefault;

/**
 * Storage for command records based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @author Dmytro Dashenkov
 * @see DatastoreStorageFactory
 */
@SuppressWarnings("WeakerAccess")   // Part of API
public class DsCommandStorage extends CommandStorage {

    private static final TypeUrl TYPE_URL = TypeUrl.from(CommandRecord.getDescriptor());
    private static final String KIND = TypeName.from(CommandRecord.getDescriptor()).value();
    private static final String COMMAND_STATUS_PROPERTY_NAME = "command_status";

    private final DatastoreWrapper datastore;

    private static final Function<Entity, CommandRecord> RECORD_MAPPER
            = new Function<Entity, CommandRecord>() {
        @Override
        public CommandRecord apply(@Nullable Entity input) {
            checkNotNull(input);
            final CommandRecord record = Entities.entityToMessage(input, TYPE_URL);
            return record;
        }
    };

    public DsCommandStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
    }

    @Override
    protected Iterator<CommandRecord> read(CommandStatus status) {
        final Filter filter = PropertyFilter.eq(COMMAND_STATUS_PROPERTY_NAME, status.ordinal());
        final Query<Entity> query = Query.newEntityQueryBuilder()
                                         .setKind(KIND)
                                         .setFilter(filter)
                                         .build();
        final Collection<Entity> entities = datastore.read(query);
        final Collection<CommandRecord> records = Collections2.transform(entities, RECORD_MAPPER);
        return records.iterator();
    }

    @Override
    public void setOkStatus(CommandId commandId) {
        checkNotNull(commandId);

        final ProcessingStatus status = ProcessingStatus.newBuilder()
                                                        .setCode(CommandStatus.OK)
                                                        .build();
        final CommandRecord updatedRecord = read(commandId)
                .or(CommandRecord.getDefaultInstance())
                .toBuilder()
                .setStatus(status)
                .build();
        write(commandId, updatedRecord);
    }

    @Override
    public void updateStatus(CommandId commandId, Error error) {
        checkNotNull(commandId);
        checkNotNull(error);

        final ProcessingStatus status = ProcessingStatus.newBuilder()
                                                        .setCode(CommandStatus.ERROR)
                                                        .setError(error)
                                                        .build();
        final CommandRecord updatedRecord = read(commandId)
                .or(CommandRecord.getDefaultInstance())
                .toBuilder()
                .setStatus(status)
                .build();
        write(commandId, updatedRecord);
    }

    @Override
    public void updateStatus(CommandId commandId, Failure failure) {
        checkNotNull(commandId);
        checkNotNull(failure);

        final ProcessingStatus status = ProcessingStatus.newBuilder()
                .setCode(CommandStatus.FAILURE)
                .setFailure(failure)
                .build();
        final CommandRecord updatedRecord = read(commandId)
                .or(CommandRecord.getDefaultInstance())
                .toBuilder()
                .setStatus(status)
                .build();
        write(commandId, updatedRecord);
    }

    @Override
    public Iterator<CommandId> index() {
        checkNotClosed();
        return Indexes.indexIterator(datastore, KIND, CommandId.class);
    }

    @Override
    public Optional<CommandRecord> read(CommandId commandId) {
        checkNotClosed();
        checkNotDefault(commandId);

        final Key key = DatastoreIdentifiers.keyFor(datastore, KIND, of(commandId));
        final Entity entity = datastore.read(key);

        if (entity == null) {
            return Optional.absent();
        }
        final CommandRecord record = RECORD_MAPPER.apply(entity);
        checkNotNull(record);
        return Optional.of(record);
    }

    @Override
    public void write(CommandId commandId, CommandRecord record) {
        checkNotClosed();
        checkNotDefault(commandId);
        checkNotDefault(record);

        final Key key = DatastoreIdentifiers.keyFor(datastore, KIND, of(commandId));

        Entity entity = messageToEntity(record, key);
        entity = Entity.newBuilder(entity)
                       .set(timestamp.toString(), record.getTimestamp()
                                                        .getSeconds())
                       .set(timestamp_nanos.toString(), record.getTimestamp()
                                                              .getNanos())
                       .set(COMMAND_STATUS_PROPERTY_NAME, record.getStatus().getCodeValue())
                       .build();
        datastore.createOrUpdate(entity);
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
}
