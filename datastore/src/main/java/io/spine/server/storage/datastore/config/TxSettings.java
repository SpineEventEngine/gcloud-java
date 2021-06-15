/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore.config;

import com.google.protobuf.Message;
import io.spine.annotation.Internal;

import java.util.Optional;

/**
 * The settings of transactional behavior for storage implementations available
 * through the {@linkplain io.spine.server.storage.datastore.DatastoreStorageFactory Datastore
 * storage factory}.
 *
 * <p>This type is internal. The library users may enable the transactional behavior for the stored
 * records via
 * {@link io.spine.server.storage.datastore.DatastoreStorageFactory.Builder#enableTransactions(Class)
 * DatastoreStorageFactory.newBuilder().enableTransactions(...)}.
 */
@Internal
public final class TxSettings
        extends Settings<TxSetting, TxSettings, TxSettings.Builder> {

    private TxSettings(Builder builder) {
        super(builder);
    }

    /**
     * Creates a new builder for this container.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Obtains the transactional setting for the storage of the records of the specified type.
     *
     * <p>If no transactional setting was not customized by the library user,
     * transactions are {@linkplain TxSetting#disabled() disabled}.
     *
     * @param recordType
     *         the type of records
     * @param <R>
     *         the type of records, as a bounding generic parameter
     * @return the transactional setting for the storage
     */
    public <R extends Message> TxSetting find(Class<R> recordType) {
        Optional<TxSetting> optional = findValue(recordType);
        TxSetting result = optional.orElseGet(TxSetting::disabled);
        return result;
    }

    /**
     * A builder of {@code TxSettings}.
     */
    public static final class Builder
            extends Settings.Builder<TxSetting, TxSettings, TxSettings.Builder> {

        /**
         * Prevents this builder from direct instantiation.
         */
        private Builder() {
            super();
        }

        @Override
        public final TxSettings build() {
            return new TxSettings(this);
        }

        @Override
        Builder self() {
            return this;
        }
    }
}
