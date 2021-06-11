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

import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * The record layouts of storage implementations available
 * to the {@linkplain io.spine.server.storage.datastore.DatastoreStorageFactory Datastore
 * storage factory}.
 *
 * <p>This type is internal. The library users are able to supply the custom layout
 * for the stored records via
 * {@link io.spine.server.storage.datastore.DatastoreStorageFactory.Builder#organizeRecords(Class,
 * RecordLayout)
 * DatastoreStorageFactory.newBuilder().organizeRecords(...)}.
 */
@Internal
public final class RecordLayouts
        extends Settings<RecordLayout<?, ?>, RecordLayouts, RecordLayouts.Builder> {

    private RecordLayouts(Builder builder) {
        super(builder);
    }

    /**
     * Creates a new {@code Builder} for this type.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Obtains the record layout for the storage of the records of the specified type.
     *
     * <p>If a custom layout was not provided by the library user,
     * {@linkplain FlatLayout a flat layout} is used.
     *
     * @param domainType
     *         the type of records
     * @param <I>
     *         the type of record identifiers
     * @param <R>
     *         the type of records, as a bounding generic parameter
     * @return the record layout for the storage
     * @throws IllegalArgumentException
     *         if the record layout of wrong type was supplied on the configuration stage
     * @implNote This method relies on the fact that the layout registered for the
     *         storage of {@code <R>}-typed records with {@code <I>}-typed identifiers has the
     *         corresponding generic parameters. Therefore, a plain class-cast is performed
     *         when obtaining. If that fails, an exception is thrown.
     */
    public <I, R extends Message> RecordLayout<I, R> find(Class<? extends Message> domainType) {
        Optional<RecordLayout<?, ?>> optional = findValue(domainType);
        if (!optional.isPresent()) {
            return new FlatLayout<>(domainType);
        }
        RecordLayout<?, ?> raw = optional.get();
        RecordLayout<I, R> result = cast(raw, domainType);
        return result;
    }

    /**
     * Casts the passed record layout to the instance with types of identifiers and records bound
     * to the values of the contextual generic parameters.
     *
     * <p>This method throws an {@code IllegalArgumentException} in case the passed value
     * cannot be cast — as its actual generic parameters may differ from expected.
     *
     * @param raw
     *         the value to cast
     * @param domainType
     *         the type of records for which the layout is set
     * @param <I>
     *         the type of record identifiers
     * @param <R>
     *         the type of records
     * @return the cast value
     * @throws IllegalArgumentException
     *         in case the value cannot be cast
     * @implNote To simplify the implementation, the cast operation is performed in
     *         a straightforward manner, and all corresponding compiler warnings are suppressed.
     */
    @SuppressWarnings({"unchecked", "OverlyBroadCatchBlock"})
    private static <I, R extends Message>
    RecordLayout<I, R> cast(RecordLayout<?, ?> raw, Class<?> domainType) {
        RecordLayout<I, R> layout;
        try {
            layout = (RecordLayout<I, R>) raw;
        } catch (Exception e) {
            throw newIllegalArgumentException(e,
                                              "Error using the provided storage layout" +
                                                      " for type `%s`.", domainType);
        }
        return layout;
    }

    /**
     * A builder for {@code RecordLayouts}.
     */
    public static final class Builder
            extends Settings.Builder<RecordLayout<?, ?>, RecordLayouts, RecordLayouts.Builder> {

        private Builder() {
            super();
        }

        @Override
        Builder self() {
            return this;
        }

        /**
         * Creates a new instance of {@code RecordLayouts} on top of this {@code Builder} instance.
         */
        @Override
        public final RecordLayouts build() {
            return new RecordLayouts(this);
        }
    }
}
