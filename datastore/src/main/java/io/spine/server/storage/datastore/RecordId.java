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

import io.spine.string.Stringifiers;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * A wrapper type for the {@code String}-based record identifiers in the GAE Datastore.
 */
final class RecordId extends DsIdentifier {

    private static final long serialVersionUID = 0L;

    /**
     * Creates a new {@code RecordId} for the given {@code value}.
     *
     * @param value
     *         the identity as {@code String} to wrap into an identifier
     */
    RecordId(String value) {
        super(value);
    }

    static RecordId of(String value) {
        checkArgument(!value.isEmpty());
        return new RecordId(value);
    }

    /**
     * Creates an instance of {@code RecordId} for a
     * given {@link io.spine.server.entity.Entity} identifier.
     *
     * @param id
     *         an identifier of an {@code Entity}
     * @return the Datastore record identifier
     */
    static RecordId ofEntityId(Object id) {
        String idAsString = Stringifiers.toString(id);
        return of(idAsString);
    }

    /**
     * Creates an instance by combining a given {@link io.spine.server.entity.Entity}
     * identifier along with the specified prefix.
     *
     * @param id
     *         an identifier of an {@code Entity}
     * @param prefix
     *         a prefix for the record identifier
     * @return the Datastore record identifier
     */
    static RecordId withPrefix(Object id, String prefix) {
        String idAsString = Stringifiers.toString(id);
        String datastoreId = format("%s_%s", prefix, idAsString);
        return of(datastoreId);
    }
}
