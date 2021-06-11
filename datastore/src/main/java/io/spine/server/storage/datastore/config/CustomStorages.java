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
import io.spine.server.storage.RecordSpec;

import java.util.Optional;

import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * A set of functions supplying the custom storage implementations, as specified
 * by library end-users.
 *
 * <p>This type is internal. The library users are able to supply the custom storage implementation
 * per stored record type via
 * {@link io.spine.server.storage.datastore.DatastoreStorageFactory.Builder#useCustomStorage(Class, CreateMessageStorage)
 * DatastoreStorageFactory.newBuilder().useCustomStorage(MessageClass, CreateMessageStorage)} for
 * plain records
 * and {@link io.spine.server.storage.datastore.DatastoreStorageFactory.Builder#useCustomStorage(Class, CreateEntityStorage)
 * DatastoreStorageFactory.newBuilder().useCustomStorage(EntityStateClass, CreateEntityStorage)}
 * for Entities.
 */
@Internal
public final class CustomStorages
        extends Settings<CreateStorage<?, ?>, CustomStorages, CustomStorages.Builder> {

    private CustomStorages(Builder builder) {
        super(builder);
    }

    /**
     * Finds a callback to create a custom storage basing on the specification of the record
     * for which a custom storage is to be created.
     *
     * @param spec
     *         the specification of the record
     * @param <I>
     *         the type of identifiers of the stored record
     * @param <R>
     *         the type of stored records
     * @return a callback to create a custom storage wrapped into {@code Optional},
     *         or {@code Optional.empty()} in case none was found
     */
    public <I, R extends Message> Optional<CreateStorage<I, R>> find(RecordSpec<I, R, ?> spec) {
        Class<? extends Message> domainType = spec.sourceType();
        Optional<CreateStorage<?, ?>> optional = findValue(domainType);
        Optional<CreateStorage<I, R>> result =
                optional.map(callback -> cast(callback, spec.idType(), domainType));
        return result;
    }

    @SuppressWarnings("unchecked")      /* Handling the exception just below. */
    private static <I, R extends Message>
    CreateStorage<I, R> cast(CreateStorage<?, ?> fn,
                             Class<I> idType, Class<? extends Message> domainType) {
        CreateStorage<I, R> result;
        try {
            result = (CreateStorage<I, R>) fn;
        } catch (ClassCastException e) {
            throw newIllegalStateException(
                    e, "Cannot cast the custom storage function" +
                            " to the expected type `CreateStorage<%s, %s>`",
                    idType.getName(), domainType.getName()
            );
        }
        return result;
    }

    /**
     * Creates a new {@code Builder} instance for this type.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder
            extends Settings.Builder<CreateStorage<?, ?>, CustomStorages, CustomStorages.Builder> {

        private Builder() {
            super();
        }

        @Override
        Builder self() {
            return this;
        }

        /**
         * Builds a new instance of {@link CustomStorages} from the callbacks configured via
         * this builder.
         */
        @Override
        public final CustomStorages build() {
            return new CustomStorages(this);
        }
    }
}
