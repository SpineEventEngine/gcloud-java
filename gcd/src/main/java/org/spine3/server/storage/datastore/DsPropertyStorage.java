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
import com.google.common.base.Optional;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.TypeUrl;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.datastore.Entities.entityToMessage;
import static org.spine3.server.storage.datastore.Entities.messageToEntity;

/**
 * Special Storage type for storing and retrieving global properties with unique keys.
 *
 * @author Mikhail Mikhaylov
 */
@SuppressWarnings("WeakerAccess")   // Part of API
public class DsPropertyStorage {

    private static final TypeUrl ANY_TYPE_URL = TypeUrl.from(Any.getDescriptor());
    private static final String KIND = Any.class.getName();

    private final DatastoreWrapper datastore;

    static DsPropertyStorage newInstance(DatastoreWrapper datastore) {
        return new DsPropertyStorage(datastore);
    }

    private DsPropertyStorage(DatastoreWrapper datastore) {
        this.datastore = datastore;
    }

    protected <V extends Message> void write(DatastoreRecordId propertyId, V value) {
        checkNotNull(propertyId);
        checkNotNull(value);

        final Key key = DatastoreIdentifiers.keyFor(datastore, KIND, propertyId);

        final Entity entity = messageToEntity(AnyPacker.pack(value), key);
        datastore.createOrUpdate(entity);
    }

    protected <V extends Message> Optional<V> read(DatastoreRecordId propertyId) {
        final Key key = DatastoreIdentifiers.keyFor(datastore, KIND, propertyId);
        final Entity response = datastore.read(key);

        if (response == null) {
            return Optional.absent();
        }

        final Any anyResult = entityToMessage(response, ANY_TYPE_URL);
        final V result = AnyPacker.unpack(anyResult);
        return Optional.fromNullable(result);
    }
}
