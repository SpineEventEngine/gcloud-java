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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.FieldMask;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.protobuf.TypeUrl;

import javax.annotation.Nullable;

import java.util.Map;

import static com.google.api.services.datastore.DatastoreV1.*;
import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.datastore.DatastoreWrapper.entityToMessage;
import static org.spine3.server.storage.datastore.DatastoreWrapper.messageToEntity;
import static org.spine3.base.Identifiers.idToString;

/**
 * {@link RecordStorage} implementation based on Google App Engine Datastore.
 *
 * @author Alexander Litus
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsEntityStorage<I> extends RecordStorage<I> {

    private final DatastoreWrapper datastore;
    private final TypeUrl typeName;
    private final TypeUrl entityStorageRecordTypeName = TypeUrl.of(EntityStorageRecord.getDescriptor());

    /* package */ static <I> DsEntityStorage<I> newInstance(Descriptor descriptor, DatastoreWrapper datastore) {
        return new DsEntityStorage<>(descriptor, datastore);
    }

    /**
     * Creates a new storage instance.
     *
     * @param descriptor the descriptor of the type of messages to save to the storage.
     * @param datastore  the datastore implementation to use.
     */
    private DsEntityStorage(Descriptor descriptor, DatastoreWrapper datastore) {
        super(false); // TODO:05-10-16:dmytro.dashenkov: Implement multitenancy.
        this.typeName = TypeUrl.of(descriptor);
        this.datastore = datastore;
    }

    @Nullable
    @Override
    protected EntityStorageRecord readInternal(I id) {
        final String idString = idToString(id);
        final Key.Builder key = createKey(idString);
        final LookupRequest request = LookupRequest.newBuilder().addKey(key).build();

        final LookupResponse response = datastore.lookup(request);

        if (response == null || response.getFoundCount() == 0) {
            return EntityStorageRecord.getDefaultInstance();
        }

        final EntityResult entity = response.getFound(0);

        final EntityStorageRecord result = entityToMessage(entity, entityStorageRecordTypeName.value());
        return result;
    }

    // TODO:05-10-16:dmytro.dashenkov: Implement.

    @Override
    protected Iterable<EntityStorageRecord> readBulkInternal(Iterable<I> ids) {
        return null;
    }

    @Override
    protected Iterable<EntityStorageRecord> readBulkInternal(Iterable<I> ids, FieldMask fieldMask) {
        return null;
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllInternal() {
        return null;
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllInternal(FieldMask fieldMask) {
        return null;
    }

    @Override
    protected void writeInternal(I id, EntityStorageRecord entityStorageRecord) {
        checkNotNull(id, "Id is null.");
        checkNotNull(entityStorageRecord, "Message is null.");

        final String idString = idToString(id);
        final Key.Builder key = createKey(idString);
        final Entity.Builder entity = messageToEntity(entityStorageRecord, key);
        final Mutation.Builder mutation = Mutation.newBuilder().addUpsert(entity);
        datastore.commit(mutation);
    }

    private Key.Builder createKey(String idString) {
        return makeKey(typeName.getSimpleName(), idString);
    }
}
