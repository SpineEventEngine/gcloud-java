/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Value;
import com.google.protobuf.Any;
import io.spine.client.Filter;
import io.spine.protobuf.TypeConverter;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnMapping;
import io.spine.server.entity.storage.ColumnTypeMapping;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link Filter} to {@link Value} adapter based on {@link ColumnMapping}.
 */
final class FilterAdapter {

    private final ColumnMapping<Value<?>> columnMapping;

    static FilterAdapter of(ColumnMapping<Value<?>> columnMapping) {
        return new FilterAdapter(columnMapping);
    }

    private FilterAdapter(ColumnMapping<Value<?>> columnMapping) {
        this.columnMapping = columnMapping;
    }

    /**
     * Extracts the filter parameter from the given {@link Filter} and converts it into
     * the Datastore {@link Value}.
     *
     * @param column
     *         the {@link Column} targeted by the given filter
     * @param columnFilter
     *         the filter
     * @return new instance of {@link Value} representing the value of the given filter
     */
    Value<?> toValue(Column column, Filter columnFilter) {
        checkNotNull(column);
        checkNotNull(columnFilter);

        Any filterValue = columnFilter.getValue();
        Class<?> columnClass = column.type();
        Object filterValueUnpacked = TypeConverter.toObject(filterValue, columnClass);

        ColumnTypeMapping<?, ? extends Value<?>> typeMapping =
                columnMapping.of(filterValueUnpacked.getClass());

        Value<?> result = typeMapping.applyTo(filterValueUnpacked);
        return result;
    }
}
