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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.empty;

/**
 * An abstract base for types which declare various settings for storage implementations.
 *
 * @param <S>
 *         the type of settings
 * @param <T>
 *         the type of concrete settings class
 * @param <B>
 *         the type of the builder of the {@code T}-typed settings class
 */
class Settings<S, T, B> {

    /**
     * Values of settings per the type of the records served by the configured storage.
     */
    private final ImmutableMap<Class<? extends Message>, S> values;

    /**
     * Create the settings instance on top of the passed builder.
     */
    Settings(Builder<S, T, B> builder) {
        values = builder.collect();
    }

    final Optional<S> findValue(Class<? extends Message> type) {

        @Nullable S value = values.get(type);
        if (value == null) {
            return empty();
        }
        return Optional.of(value);
    }

    /**
     * The abstract base for settings builders.
     *
     * @param <S>
     *         the type of settings
     * @param <T>
     *         the type of concrete settings class
     * @param <B>
     *         the type of the concrete descendant of this {@code Builder} type
     */
    abstract static class Builder<T, S, B> {

        private final Map<Class<? extends Message>, T> values = new HashMap<>();

        /**
         * Builds a new instance of settings from the settings values configured via this builder.
         */
        public abstract S build();

        /**
         * Return the type of a particular builder implementation, for call chaining.
         */
        abstract B asThis();

        /**
         * Adds a setting the specific type of stored record.
         *
         * <p>Each next value added for the same stored record type overwrites
         * the previous value.
         *
         * @param recordType
         *         the type of record which storage is configured by the passed setting
         * @param value
         *         a setting value
         * @param <R>
         *         the type of the record, as a generic parameter
         * @return this instance of {@code Builder} for call chaining
         */
        @CanIgnoreReturnValue
        public <R extends Message> B add(Class<R> recordType, T value) {
            checkNotNull(recordType);
            checkNotNull(value);
            values.put(recordType, value);
            return asThis();
        }

        /**
         * Tells whether this builder already has a setting value for the passed record type.
         *
         * @param recordType
         *         the type of record
         * @return {@code true} if this builder already has a configured value,
         *         {@code false} otherwise
         */
        public boolean has(Class<?> recordType) {
            checkNotNull(recordType);
            return values.containsKey(recordType);
        }

        private ImmutableMap<Class<? extends Message>, T> collect() {
            ImmutableMap.Builder<Class<? extends Message>, T> builder = ImmutableMap.builder();
            for (Class<? extends Message> recordType : values.keySet()) {
                T value = values.get(recordType);
                builder.put(recordType, value);
            }
            ImmutableMap<Class<? extends Message>, T> result = builder.build();
            return result;
        }
    }
}
