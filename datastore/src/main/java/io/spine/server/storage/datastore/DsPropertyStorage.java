/*
 * Copyright 2022, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.type.TypeUrl;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.storage.datastore.Entities.fromMessage;
import static io.spine.server.storage.datastore.Entities.toMessage;

/**
 * Special Storage type for storing and retrieving global properties with unique keys.
 */
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

        Descriptor typeDescriptor = value.getDescriptorForType();
        Kind kind = Kind.of(typeDescriptor);

        Key key = datastore.keyFor(kind, propertyId);

        Entity entity = fromMessage(pack(value), key);
        datastore.createOrUpdate(entity);
    }

    protected Optional<Message> read(RecordId propertyId, Descriptor targetType) {
        checkNotNull(propertyId);
        checkNotNull(targetType);

        Kind kind = Kind.of(targetType);

        Key key = datastore.keyFor(kind, propertyId);
        Entity response = datastore.read(key);

        if (response == null) {
            return Optional.empty();
        }

        Any anyResult = toMessage(response, ANY_TYPE_URL);
        Message result = unpack(anyResult);
        return Optional.ofNullable(result);
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
