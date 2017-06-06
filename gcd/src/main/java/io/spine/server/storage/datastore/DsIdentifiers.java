/*
 *
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
 *
 */
package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import io.spine.string.Stringifiers;

import static com.google.common.base.Preconditions.checkState;

/**
 * Utilities for working with GAE Datastore record identifiers and keys.
 *
 * @author Alex Tymchenko
 */
@SuppressWarnings({
        "UtilityClass",
        "WeakerAccess"  /* as it's part of API */})
public class DsIdentifiers {

    private DsIdentifiers() {
    }

    /**
     * Creates an instance of {@link com.google.cloud.datastore.Key} basing on the Datastore
     * entity {@code kind} and {@code recordId}.
     *
     * @param datastore the instance of {@code datastore} to create a {@code Key} for
     * @param kind      the kind of the Datastore entity
     * @param recordId  the ID of the record
     * @return the Datastore {@code Key} instance
     */
    static Key keyFor(DatastoreWrapper datastore, Kind kind, DatastoreRecordId recordId) {
        final KeyFactory keyFactory = datastore.getKeyFactory(kind);
        final Key key = keyFactory.newKey(recordId.getValue());

        return key;
    }

    public static DatastoreRecordId of(String value) {
        checkState(!value.isEmpty());
        return new DatastoreRecordId(value);
    }

    /**
     * Creates an instance of {@code DatastoreRecordId} for a
     * given {@link io.spine.server.entity.Entity} identifier.
     *
     * @param id an identifier of an {@code Entity}
     * @return the Datastore record identifier
     */
    public static DatastoreRecordId ofEntityId(Object id) {
        final String idAsString = Stringifiers.toString(id);
        return of(idAsString);
    }
}
