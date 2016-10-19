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
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.storage.datastore.newapi.DatastoreWrapper;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.datastore.newapi.Entities.entityToMessage;
import static org.spine3.server.storage.datastore.newapi.Entities.messageToEntity;

/**
 * Special Storage type for storing and retrieving global properties with unique keys.
 *
 * @author Mikhail Mikhaylov
 */
/* package */ class DsPropertyStorage {

    private static final TypeUrl ANY_TYPE_URL = TypeUrl.of(Any.getDescriptor());

    private final DatastoreWrapper datastore;

    /* package */
    static DsPropertyStorage newInstance(DatastoreWrapper datastore) {
        return new DsPropertyStorage(datastore);
    }

    private DsPropertyStorage(DatastoreWrapper datastore) {
        this.datastore = datastore;
    }

    /* package */ <V extends Message> void write(String propertyId, V value) {
        checkNotNull(propertyId);
        checkNotNull(value);

        final Key key = datastore.getKeyFactory(ANY_TYPE_URL.getTypeName()).newKey(propertyId);
        final Entity entity = messageToEntity(AnyPacker.pack(value), key);
        datastore.createOrUpdate(entity);
    }

    @Nullable
    /* package */ <V extends Message> V read(String propertyId) {
        final Key key = datastore.getKeyFactory(ANY_TYPE_URL.getTypeName()).newKey(propertyId);

        final Entity response = datastore.read(key);

        if (response == null) {
            return null;
        }

        final Any anyResult = entityToMessage(response, ANY_TYPE_URL);
        return AnyPacker.unpack(anyResult);
    }
}
