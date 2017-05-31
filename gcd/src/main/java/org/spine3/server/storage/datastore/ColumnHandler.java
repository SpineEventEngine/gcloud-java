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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.Value;
import com.google.protobuf.Any;
import org.spine3.client.ColumnFilter;
import org.spine3.protobuf.TypeConverter;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.ColumnTypeRegistry;
import org.spine3.server.storage.datastore.type.DatastoreColumnType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dmytro Dashenkov
 */
final class ColumnHandler {

    private final ColumnTypeRegistry<? extends DatastoreColumnType> registry;

    ColumnHandler(ColumnTypeRegistry<? extends DatastoreColumnType> registry) {
        this.registry = registry;
    }

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
