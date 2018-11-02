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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.protobuf.FieldMask;
import io.spine.client.OrderBy;
import io.spine.server.entity.EntityRecord;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;
import static io.spine.server.entity.EntityWithLifecycle.Predicates.isRecordActive;
import static io.spine.server.storage.datastore.DsEntityComparator.implementing;
import static io.spine.server.storage.datastore.FieldMaskApplier.maskNullableRecord;
import static io.spine.server.storage.datastore.FieldMaskApplier.maskRecord;
import static io.spine.server.storage.datastore.RecordId.ofEntityId;
import static java.util.stream.Collectors.toList;

/**
 * An {@code Entity} lookup in Google Datastore using {@code Entity} identifiers.
 *
 * @implNote Lookup is performed by reading all the entities with Datastore Keys matching
 *         the provided IDs first and then applying other query constraints in-memory.
 */
final class DsLookupByIds<I> {

    private final DatastoreWrapper datastore;
    private final TypeUrl typeUrl;

    DsLookupByIds(DatastoreWrapper datastore, TypeUrl url) {
        this.datastore = datastore;
        this.typeUrl = url;
    }

    /**
     * Queries at most the specified amount of records with supplied identifiers which match the
     * provided predicate, and applies a field mask to the query results.
     *
     * <p>The results are returned in an order specified by the provided
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
    Iterator<EntityRecord> find(Iterable<I> ids,
                                FieldMask fieldMask,
                                Predicate<Entity> predicate,
                                OrderBy orderBy,
                                long limit) {
        return read(ids)
                .filter(Objects::nonNull)
                .filter(predicate)
                .sorted(implementing(orderBy))
                .map(Entities::toRecord)
                .map(maskRecord(fieldMask))
                .limit(limit)
                .iterator();
    }

    /**
     * Queries the records with supplied identifiers which match the provided predicate,
     * and applies a field mask to the query results.
     *
     * <p>The results are returned in an order specified by the provided
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
    Iterator<EntityRecord> find(Iterable<I> ids,
                                FieldMask fieldMask,
                                Predicate<Entity> predicate,
                                OrderBy orderBy) {
        return read(ids)
                .filter(Objects::nonNull)
                .filter(predicate)
                .sorted(implementing(orderBy))
                .map(Entities::toRecord)
                .map(maskRecord(fieldMask))
                .iterator();
    }

    /**
     * Queries the records with supplied identifiers which match the provided predicate,
     * and applies a field mask to the query results.
     *
     * <p>The order of the results is not guaranteed.
     *
     * @param ids
     *         entity identifiers which are translated to Datastore keys
     * @param fieldMask
     *         a field mask specifying fields to be included in resulting entities
     * @param predicate
     *         a predicate which must be matched by entities to be returned as results
     * @return an iterator over the matching entity records
     */
    Iterator<EntityRecord> find(Iterable<I> ids,
                                FieldMask fieldMask,
                                Predicate<Entity> predicate) {
        return read(ids)
                .filter(Objects::nonNull)
                .filter(predicate)
                .map(Entities::toRecord)
                .map(maskRecord(fieldMask))
                .iterator();
    }

    /**
     * Queries the records with supplied identifiers and applies a field mask to the query results.
     *
     * <p>The results are returned in an order matching that of the provided IDs with nulls
     * in place of missing and inactive entities.
     *
     * @param ids
     *         entity identifiers which are translated to Datastore keys
     * @param fieldMask
     *         a field mask specifying fields to be included in resulting entities
     * @return an iterator over the matching nullable entity records
     */
    Iterator<@Nullable EntityRecord> findActive(Iterable<I> ids, FieldMask fieldMask) {
        return read(ids)
                .map(Entities::nullableToRecord)
                .map(nullIfNot(isRecordActive()))
                .map(maskNullableRecord(fieldMask))
                .iterator();
    }

    /**
     * Queries the records with supplied identifiers.
     *
     * <p>The results are returned in an order matching that of the provided IDs with nulls
     * in place of missing and inactive entities.
     *
     * @param ids
     *         entity identifiers which are translated to Datastore keys
     * @return an iterator over the matching nullable entity records
     */
    Iterator<@Nullable EntityRecord> findActive(Iterable<I> ids) {
        return read(ids)
                .map(Entities::nullableToRecord)
                .map(nullIfNot(isRecordActive()))
                .iterator();
    }

    private static <O> Function<O, @Nullable O> nullIfNot(Predicate<O> predicate) {
        return obj -> predicate.test(checkNotNull(obj)) ? obj : null;
    }

    private Stream<@Nullable Entity> read(Iterable<I> ids) {
        Collection<Key> keys = toKeys(ids);
        Stream<@Nullable Entity> entities = stream(datastore.read(keys));
        return entities;
    }

    private Collection<Key> toKeys(Iterable<I> ids) {
        return stream(ids)
                .map(id -> datastore.keyFor(Kind.of(typeUrl), ofEntityId(id)))
                .collect(toList());
    }
}
