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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.base.Optional;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.protobuf.AnyPacker;
import io.spine.type.TypeUrl;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.datastore.Entities.entityToMessage;
import static io.spine.server.storage.datastore.Entities.messageToEntity;

/**
 * Special Storage type for storing and retrieving global properties with unique keys.
 *
 * @author Mikhail Mikhaylov
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("WeakerAccess")   // Part of API
public class DsPropertyStorage {

    private static final TypeUrl ANY_TYPE_URL = TypeUrl.from(Any.getDescriptor());

    private final DatastoreWrapper datastore;

    static DsPropertyStorage newInstance(DatastoreWrapper datastore) {
        return new DsPropertyStorage(datastore);
    }

    private DsPropertyStorage(DatastoreWrapper datastore) {
        this.datastore = datastore;
    }

    protected <V extends Message> void write(RecordId propertyId, V value) {
        checkNotNull(propertyId);
        checkNotNull(value);

        final Descriptor typeDescriptor = value.getDescriptorForType();
        final Kind kind = Kind.of(typeDescriptor);

        final Key key = DsIdentifiers.keyFor(datastore, kind, propertyId);

        final Entity entity = messageToEntity(AnyPacker.pack(value), key);
        datastore.createOrUpdate(entity);
    }

    protected <V extends Message> Optional<V> read(RecordId propertyId,
                                                   Descriptor targetTypeDescriptor) {
        checkNotNull(propertyId);
        checkNotNull(targetTypeDescriptor);

        final Kind kind = Kind.of(targetTypeDescriptor);

        final Key key = DsIdentifiers.keyFor(datastore, kind, propertyId);
        final Entity response = datastore.read(key);

        if (response == null) {
            return Optional.absent();
        }

        final Any anyResult = entityToMessage(response, ANY_TYPE_URL);
        final V result = AnyPacker.unpack(anyResult);
        return Optional.fromNullable(result);
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
