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
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Value;
import com.google.cloud.datastore.ValueType;
import com.google.protobuf.FieldMask;
import io.spine.client.OrderBy;
import io.spine.server.entity.EntityRecord;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Streams.stream;
import static io.spine.client.OrderBy.Direction.ASCENDING;
import static io.spine.server.storage.datastore.DsIdentifiers.keyFor;
import static io.spine.server.storage.datastore.DsIdentifiers.ofEntityId;
import static io.spine.server.storage.datastore.DsQueryHelper.maskRecord;
import static io.spine.server.storage.datastore.Entities.activeEntity;
import static io.spine.util.Exceptions.newIllegalStateException;
import static io.spine.validate.Validate.checkNotDefault;

/**
 * An {@code Entity} lookup in Google Datastore using {@code Entity} identifiers.
 *
 * @author Mykhailo Drachuk
 */
final class DsIdLookup<I> {

    private final DatastoreWrapper datastore;
    private final TypeUrl typeUrl;

    DsIdLookup(DatastoreWrapper datastore, TypeUrl url) {
        this.datastore = datastore;
        this.typeUrl = url;
    }

    /**
     * Queries the specified amount of records with supplied identifiers which match the
     * provided predicate, and applies a field mask to the query results.
     *
     * The results are returned in an order specified by the provided
     * {@linkplain io.spine.client.OrderBy order clause}.
     *
     * @param ids
     *         entity identifiers which are translated to Datastore keys
     * @param fieldMask
     *         a field mask specifying fields to be included in resulting entities
     * @param predicate
     *         a predicate which must be matched by entities to be returned as results
     * @param orderBy
     *         a specification of order in which the query results must be returned
     * @return an iterator over the matching entity records
     */
    Iterator<EntityRecord> execute(Iterable<I> ids, FieldMask fieldMask,
                                   Predicate<Entity> predicate, OrderBy orderBy,
                                   long limit) {
        return read(ids)
                .filter(predicate)
                .sorted(RecordComparator.implementing(orderBy))
                .map(DsQueryHelper::toRecord)
                .map(maskRecord(typeUrl, fieldMask))
                .limit(limit)
                .iterator();
    }

    /**
     * Queries the records with supplied identifiers which match the provided predicate,
     * and applies a field mask to the query results.
     *
     * The results are returned in an order specified by the provided
     * {@linkplain io.spine.client.OrderBy order clause}.
     *
     * @param ids
     *         entity identifiers which are translated to Datastore keys
     * @param fieldMask
     *         a field mask specifying fields to be included in resulting entities
     * @param predicate
     *         a predicate which must be matched by entities to be returned as results
     * @param orderBy
     *         a specification of order in which the query results must be returned
     * @return an iterator over the matching entity records
     */
    Iterator<EntityRecord> execute(Iterable<I> ids, FieldMask fieldMask,
                                   Predicate<Entity> predicate, OrderBy orderBy) {
        return read(ids)
                .filter(predicate)
                .sorted(RecordComparator.implementing(orderBy))
                .map(DsQueryHelper::toRecord)
                .map(maskRecord(typeUrl, fieldMask))
                .iterator();
    }

    /**
     * Queries the records with supplied identifiers which match the provided predicate,
     * and applies a field mask to the query results.
     *
     * @param ids
     *         entity identifiers which are translated to Datastore keys
     * @param fieldMask
     *         a field mask specifying fields to be included in resulting entities
     * @param predicate
     *         a predicate which must be matched by entities to be returned as results
     * @return an iterator over the matching entity records
     */
    Iterator<EntityRecord> execute(Iterable<I> ids,
                                   FieldMask fieldMask,
                                   Predicate<Entity> predicate) {
        return read(ids)
                .filter(predicate)
                .map(DsQueryHelper::toRecord)
                .map(maskRecord(typeUrl, fieldMask))
                .iterator();
    }

    /**
     * Queries the records with supplied identifiers and applies a field mask to the query results.
     *
     * @param ids
     *         entity identifiers which are translated to Datastore keys
     * @param fieldMask
     *         a field mask specifying fields to be included in resulting entities
     * @return an iterator over the matching entity records
     */
    Iterator<EntityRecord> execute(Iterable<I> ids, FieldMask fieldMask) {
        return read(ids)
                .filter(activeEntity())
                .map(DsQueryHelper::toRecord)
                .map(maskRecord(typeUrl, fieldMask))
                .iterator();
    }

    /**
     * Queries the records with supplied identifiers.
     *
     * @param ids
     *         entity identifiers which are translated to Datastore keys
     * @return an iterator over the matching entity records
     */
    Iterator<EntityRecord> execute(Iterable<I> ids) {
        return read(ids)
                .filter(activeEntity())
                .map(DsQueryHelper::toRecord)
                .iterator();
    }

    private Stream<Entity> read(Iterable<I> ids) {
        Collection<Key> keys = toKeys(ids);
        return stream(datastore.read(keys));
    }

    private Collection<Key> toKeys(Iterable<I> ids) {
        Collection<Key> keys = newLinkedList();
        for (I id : ids) {
            Key key = keyFor(datastore, kindFrom(typeUrl), ofEntityId(id));
            keys.add(key);
        }
        return keys;
    }

    private static Kind kindFrom(TypeUrl typeUrl) {
        return Kind.of(typeUrl);
    }

    private static class RecordComparator implements Comparator<Entity>, Serializable {

        private static final long serialVersionUID = 0L;
        private final String column;

        private RecordComparator(String column) {
            this.column = column;
        }

        @Override
        public int compare(Entity a, Entity b) {
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

        private IllegalStateException noColumnInEntity(Entity b) {
            return newIllegalStateException("Entity %s does not contain column %s", b, column);
        }

        private static Comparator<Entity> implementing(OrderBy orderBy) {
            checkNotNull(orderBy);
            checkNotDefault(orderBy);
            Comparator<Entity> comparator = new RecordComparator(orderBy.getColumn());
            return orderBy.getDirection() == ASCENDING ? comparator : comparator.reversed();
        }
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
