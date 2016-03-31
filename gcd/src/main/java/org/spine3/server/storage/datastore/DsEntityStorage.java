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
import com.google.protobuf.Message;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;

import static com.google.api.services.datastore.DatastoreV1.*;
import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.datastore.DatastoreWrapper.entityToMessage;
import static org.spine3.server.storage.datastore.DatastoreWrapper.messageToEntity;
import static org.spine3.base.Identifiers.idToString;

/**
 * {@link EntityStorage} implementation based on Google App Engine Datastore.
 *
 * @author Alexander Litus
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsEntityStorage<I> extends EntityStorage<I> {

    private final DatastoreWrapper datastore;
    private final TypeName typeName;

    protected static <I, M extends Message> DsEntityStorage<I> newInstance(Descriptor descriptor, DatastoreWrapper datastore) {
        return new DsEntityStorage<>(descriptor, datastore);
    }

    /**
     * Creates a new storage instance.
     *
     * @param descriptor the descriptor of the type of messages to save to the storage.
     * @param datastore  the datastore implementation to use.
     */
    private DsEntityStorage(Descriptor descriptor, DatastoreWrapper datastore) {
        this.typeName = TypeName.of(descriptor);
        this.datastore = datastore;
    }

    @Nullable
    @Override
    protected EntityStorageRecord readInternal(I i) {
        final String idString = idToString(i);
        final Key.Builder key = createKey(idString);
        final LookupRequest request = LookupRequest.newBuilder().addKey(key).build();

        final LookupResponse response = datastore.lookup(request);

        if (response == null || response.getFoundCount() == 0) {
            return getEmptyRecord();
        }

        final EntityResult entity = response.getFound(0);
        final EntityStorageRecord result = entityToMessage(entity, typeName.toTypeUrl());
        return result;
    }

    @Override
    protected void writeInternal(I i, EntityStorageRecord entityStorageRecord) {
        checkNotNull(i, "Id is null.");
        checkNotNull(entityStorageRecord, "Message is null.");

        final String idString = idToString(i);
        final Key.Builder key = createKey(idString);
        final Entity.Builder entity = messageToEntity(entityStorageRecord, key);
        final Mutation.Builder mutation = Mutation.newBuilder().addInsert(entity);
        datastore.commit(mutation);
    }

    private Key.Builder createKey(String idString) {
        return makeKey(typeName.nameOnly(), idString);
    }

    private static EntityStorageRecord getEmptyRecord() {
        final EntityStorageRecord empty = EntityStorageRecord.getDefaultInstance();
        return empty;
    }
}
