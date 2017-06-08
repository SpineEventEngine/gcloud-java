/*
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
 */

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Value;
import com.google.protobuf.Any;
import io.spine.client.ColumnFilter;
import io.spine.protobuf.TypeConverter;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.datastore.type.DatastoreColumnType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link ColumnTypeRegistry} based {@link ColumnFilter} to {@link Value} adapter.
 *
 * @author Dmytro Dashenkov
 */
final class ColumnFilterAdapter {

    private final ColumnTypeRegistry<? extends DatastoreColumnType> registry;

    /**
     * Creates a new instance of {@code ColumnFilterAdapter} on top of the given
     * {@link ColumnTypeRegistry}.
     */
    static ColumnFilterAdapter of(ColumnTypeRegistry<? extends DatastoreColumnType> registry) {
        return new ColumnFilterAdapter(registry);
    }

    private ColumnFilterAdapter(ColumnTypeRegistry<? extends DatastoreColumnType> registry) {
        this.registry = registry;
    }

    /**
     * Extracts the filter parameter from the given {@link ColumnFilter} and converts it into
     * the Datastore {@link Value}.
     *
     * @param column       the {@link Column} targeted by the given filter
     * @param columnFilter the filter
     * @return new instance of {@link Value} representing the value of the given filter
     */
    Value<?> toValue(Column column, ColumnFilter columnFilter) {
        checkNotNull(column);
        checkNotNull(columnFilter);

        final DatastoreColumnType type = registry.get(column);
        checkArgument(type != null, "Column of unknown type: %s.", column);

        final Any filterValue = columnFilter.getValue();
        final Object filterValueUnpacked = TypeConverter.toObject(filterValue, column.getType());

        @SuppressWarnings("unchecked") // Concrete type is unknown on compile time.
        final Value<?> result = type.toValue(type.convertColumnValue(filterValueUnpacked));
        return result;
    }
}
