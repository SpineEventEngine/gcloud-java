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

import com.google.cloud.datastore.Value;
import com.google.protobuf.Any;
import io.spine.client.Filter;
import io.spine.protobuf.TypeConverter;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnConversionRules;
import io.spine.server.entity.storage.ConversionRule;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link Filter} to {@link Value} adapter based on {@link ColumnConversionRules}.
 */
final class FilterAdapter {

    private final ColumnConversionRules<Value<?>> conversionRules;

    static FilterAdapter of(ColumnConversionRules<Value<?>> conversionRules) {
        return new FilterAdapter(conversionRules);
    }

    private FilterAdapter(ColumnConversionRules<Value<?>> conversionRules) {
        this.conversionRules = conversionRules;
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

        ConversionRule<?, ? extends Value<?>> strategy =
                conversionRules.of(filterValueUnpacked.getClass());
        checkArgument(strategy != null, "Column of unknown type: %s.", column);

        Value<?> result = strategy.applyTo(filterValueUnpacked);
        return result;
    }
}
