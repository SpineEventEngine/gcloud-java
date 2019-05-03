/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.Value;
import com.google.protobuf.Any;
import io.spine.client.Filter;
import io.spine.protobuf.TypeConverter;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.datastore.type.DatastoreColumnType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link ColumnTypeRegistry} based {@link Filter} to {@link Value} adapter.
 */
final class FilterAdapter {

    private final ColumnTypeRegistry<? extends DatastoreColumnType> registry;

    /**
     * Creates a new instance of {@code FilterAdapter} on top of the given
     * {@link ColumnTypeRegistry}.
     */
    static FilterAdapter of(ColumnTypeRegistry<? extends DatastoreColumnType> registry) {
        return new FilterAdapter(registry);
    }

    private FilterAdapter(ColumnTypeRegistry<? extends DatastoreColumnType> registry) {
        this.registry = registry;
    }

    /**
     * Extracts the filter parameter from the given {@link Filter} and converts it into
     * the Datastore {@link Value}.
     *
     * @param column       the {@link EntityColumn} targeted by the given filter
     * @param columnFilter the filter
     * @return new instance of {@link Value} representing the value of the given filter
     */
    Value<?> toValue(EntityColumn column, Filter columnFilter) {
        checkNotNull(column);
        checkNotNull(columnFilter);

        DatastoreColumnType type = registry.get(column);
        checkArgument(type != null, "Column of unknown type: %s.", column);

        Any filterValue = columnFilter.getValue();
        Class<?> columnClass = column.getType();
        Object filterValueUnpacked = TypeConverter.toObject(filterValue, columnClass);
        Object columnValue = column.toPersistedValue(filterValueUnpacked);

        if (columnValue == null) {
            return NullValue.of();
        }

        @SuppressWarnings("unchecked") // Concrete type is unknown on compile time.
        Value<?> result = type.toValue(type.convertColumnValue(columnValue));
        return result;
    }
}
