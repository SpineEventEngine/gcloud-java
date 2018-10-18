/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Value;
import com.google.cloud.datastore.ValueType;
import io.spine.client.OrderBy;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.client.OrderBy.Direction.ASCENDING;
import static io.spine.util.Exceptions.newIllegalStateException;
import static io.spine.validate.Validate.checkNotDefault;

class DsEntityComparator implements Comparator<Entity>, Serializable {

    private static final long serialVersionUID = 0L;
    private final String column;

    private DsEntityComparator(String column) {
        this.column = column;
    }

    @Override
    public int compare(Entity a, Entity b) {
        checkNotNull(a);
        checkNotNull(b);

        Comparable aValue = ComparableValueExtractor.comparable(a.getValue(column));
        Comparable bValue = ComparableValueExtractor.comparable(b.getValue(column));

        if (aValue == null) {
            return -1;
        }
        if (bValue == null) {
            return +1;
        }

        //noinspection unchecked entity values are required to be comparable by Spine
        return aValue.compareTo(bValue);
    }

    static Comparator<Entity> implementing(OrderBy orderBy) {
        checkNotNull(orderBy);
        checkNotDefault(orderBy);
        Comparator<Entity> comparator = new DsEntityComparator(orderBy.getColumn());
        return orderBy.getDirection() == ASCENDING ? comparator : comparator.reversed();
    }

    /**
     * An extractor of comparable values from Datastore {@link Value Value}.
     *
     * <p>Only {@link ValueType#NULL NULL}, {@link ValueType#STRING STRING},
     * {@link ValueType#LONG LONG}, {@link ValueType#DOUBLE DOUBLE},
     * {@link ValueType#BOOLEAN BOOLEAN}, and {@link ValueType#TIMESTAMP TIMESTAMP} column types
     * support comparison, thus ordering.
     *
     * <p>The {@link ValueType#ENTITY ENTITY}, {@link ValueType#LIST LIST},
     * {@link ValueType#RAW_VALUE RAW_VALUE}, {@link ValueType#LAT_LNG LAT_LNG},
     * and {@link ValueType#KEY KEY} types are not supported.
     */
    private enum ComparableValueExtractor {
        NULL(ValueType.NULL) {
            @Override
            @Nullable Comparable extract(Value<?> value) {
                return null;
            }
        },
        STRING(ValueType.STRING) {
            @Override
            Comparable extract(Value<?> value) {
                return (Comparable) value.get();
            }
        },
        LONG(ValueType.LONG) {
            @Override
            @Nullable Comparable extract(Value<?> value) {
                return (Long) value.get();
            }
        },
        DOUBLE(ValueType.DOUBLE) {
            @Override
            @Nullable Comparable extract(Value<?> value) {
                return (Double) value.get();
            }
        },
        BOOLEAN(ValueType.BOOLEAN) {
            @Override
            @Nullable Comparable extract(Value<?> value) {
                return (Boolean) value.get();
            }
        },
        TIMESTAMP(ValueType.TIMESTAMP) {
            @Override
            @Nullable Comparable extract(Value<?> value) {
                return (Timestamp) value.get();
            }
        };

        private final ValueType valueType;

        ComparableValueExtractor(ValueType type) {
            this.valueType = type;
        }

        abstract @Nullable Comparable extract(Value<?> value);

        private boolean matches(ValueType type) {
            return type == valueType;
        }

        static @Nullable Comparable comparable(Value<?> value) {
            ValueType type = value.getType();
            ComparableValueExtractor extractor = pickForType(type);
            return extractor.extract(value);
        }

        private static ComparableValueExtractor pickForType(ValueType type) {
            return Arrays.stream(values())
                         .filter(extractType -> extractType.matches(type))
                         .findFirst()
                         .orElseThrow(unrecognizedType(type));
        }

        private static Supplier<IllegalStateException> unrecognizedType(ValueType type) {
            return () -> newIllegalStateException("Unrecognized Datastore type %s.", type);
        }
    }
}
