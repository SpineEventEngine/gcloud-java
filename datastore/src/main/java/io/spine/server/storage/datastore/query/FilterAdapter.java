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

package io.spine.server.storage.datastore.query;

import com.google.cloud.datastore.Value;
import io.spine.query.SubjectParameter;
import io.spine.server.storage.ColumnMapping;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@linkplain SubjectParameter query-parameter}-to-{@link Value} adapter
 * based on {@link ColumnMapping}.
 */
public final class FilterAdapter {

    private final ColumnMapping<Value<?>> columnMapping;

    public static FilterAdapter of(ColumnMapping<Value<?>> columnMapping) {
        checkNotNull(columnMapping);
        return new FilterAdapter(columnMapping);
    }

    private FilterAdapter(ColumnMapping<Value<?>> columnMapping) {
        this.columnMapping = columnMapping;
    }

    /**
     * Returns the column mapping used in transformation from the platform-specific values of
     * query parameters to Datastore-native {@link Value}s.
     */
    ColumnMapping<Value<?>> columnMapping() {
        return columnMapping;
    }

    /**
     * Extracts the specified parameter value from the given {@link SubjectParameter}
     * and converts it into the Datastore {@link Value}.
     *
     * @param parameter
     *         the parameter of the query
     * @return new instance of {@link Value} representing the value of the given parameter
     */
    Value<?> transformValue(SubjectParameter<?, ?, ?> parameter) {
        checkNotNull(parameter);
        var paramValue = parameter.value();
        var typeMapping = columnMapping.of(paramValue.getClass());
        var result = typeMapping.applyTo(paramValue);
        return result;
    }
}
