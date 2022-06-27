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
import com.google.cloud.datastore.Value;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.protobuf.Message;

import java.util.function.Function;

/**
 * An abstract base for enumerations describing the columns of plain messages {@link Message}s.
 *
 * @param <M>
 *         the type of {@code Message} stored
 */
interface MessageColumn<M extends Message> {

    /**
     * Obtains the name of the column.
     */
    String columnName();

    /**
     * Obtains the getter to read the column value from the {@code Message}.
     */
    Getter<M> getter();

    /**
     * Fills a property of the given {@code builder} with the column value obtained
     * from the given {@code message}.
     *
     * @param builder
     *         the builder to set the value to
     * @param message
     *         the message which field value is going to be set
     * @return the same instance of {@code builder} with the property filled
     */
    @CanIgnoreReturnValue
    default Entity.Builder fill(Entity.Builder builder, M message) {
        Value<?> value = getter().apply(message);
        builder.set(columnName(), value);
        return builder;
    }

    /**
     * A functional interface for routines that obtain values from the {@link Message} fields
     * for the respective {@link Entity} columns.
     */
    @Immutable
    @FunctionalInterface
    interface Getter<M extends Message> extends Function<M, Value<?>> {
    }
}
