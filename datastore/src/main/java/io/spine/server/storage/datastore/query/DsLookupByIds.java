/*
 * Copyright 2021, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.query.RecordQuery;
import io.spine.server.storage.datastore.DatastoreMedium;
import io.spine.server.storage.datastore.record.DsEntitySpec;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.spine.server.storage.datastore.record.DsEntityComparator.implementing;
import static io.spine.server.storage.datastore.record.Entities.toMessage;
import static io.spine.server.storage.datastore.record.FieldMaskApplier.recordMasker;

/**
 * An {@code Entity} lookup in Google Datastore using {@code Entity} identifiers.
 *
 * @implNote Lookup is performed by reading all the entities with Datastore Keys matching
 *         the provided IDs first and then applying other query constraints in-memory.
 */
final class DsLookupByIds<I, R extends Message> extends PreparedQuery<I, R> {

    private final DatastoreMedium datastore;

    DsLookupByIds(DatastoreMedium datastore,
                  RecordQuery<I, R> query,
                  FilterAdapter adapter,
                  DsEntitySpec<I, R> spec) {
        super(query, adapter, spec);
        this.datastore = datastore;
    }

    @Override
    public IntermediateResult fetchFromDatastore() {
        List<@Nullable Entity> rawEntities = readList(identifiers());
        return new IntermediateResult(rawEntities);
    }

    @Override
    public Iterable<R> toRecords(IntermediateResult intermediateResult) {
        List<@Nullable Entity> rawEntities = intermediateResult.entities();
        Predicate<Entity> predicate = columnPredicate();
        Stream<@Nullable Entity> stream = rawEntities
                .stream()
                .filter(Objects::nonNull)
                .filter(predicate);
        if(hasOrdering()) {
            stream = stream.sorted(implementing(sorting()));
        }
        Stream<R> recordStream = stream.map(toMaskedRecord(mask()));
        if(limit() != null && limit() > 0 ) {
            recordStream = recordStream.limit(limit());
        }
        ImmutableList<R> result = recordStream.collect(toImmutableList());
        return result;
    }

    private Predicate<Entity> columnPredicate() {
        if(!(predicate().allParams().isEmpty() && predicate().children().isEmpty())) {
            return new ColumnPredicate<>(query().subject(), columnAdapter());
        }
        return entity -> true;
    }

    private List<@Nullable Entity> readList(Iterable<I> ids) {
        ImmutableList<Key> keys = toKeys(ids);
        List<@Nullable Entity> entities = datastore.lookup(keys);
        return entities;
    }

    private ImmutableList<Key> toKeys(Iterable<I> ids) {
        ImmutableList<Key> keys = stream(ids)
                .map(id -> spec().keyOf(id, datastore))
                .collect(toImmutableList());
        return keys;
    }

    private Function<Entity, R> toMaskedRecord(FieldMask mask) {
        Function<R, R> masker = recordMasker(mask);
        return entity -> {
            R record = toMessage(entity, recordType());
            R maskedRecord = masker.apply(record);
            return maskedRecord;
        };
    }

//    /**
//     * Queries at most the specified amount of records with supplied identifiers which match the
//     * provided predicate, and applies a field mask to the query results.
//     *
//     * <p>The results are returned in an order specified by the provided
//     * {@linkplain io.spine.client.OrderBy order clause}.
//     *
//     * @param ids
//     *         entity identifiers which are translated to Datastore keys
//     * @param fieldMask
//     *         a field mask specifying fields to be included in resulting entities
//     * @param predicate
//     *         a predicate which must be matched by entities to be returned as results
//     * @param orderBy
//     *         a specification of order in which the query results must be returned
//     * @return an iterator over the matching entity records
//     */
//    Iterator<EntityRecord> find(Iterable<I> ids,
//                                FieldMask fieldMask,
//                                Predicate<Entity> predicate,
//                                OrderBy orderBy,
//                                long limit) {
//        Iterator<EntityRecord> recordIterator =
//                findSortedRecords(ids, fieldMask, predicate, orderBy)
//                        .limit(limit)
//                        .iterator();
//        return recordIterator;
//    }

//    /**
//     * Queries the records with supplied identifiers which match the provided predicate,
//     * and applies a field mask to the query results.
//     *
//     * <p>The results are returned in an order specified by the provided
//     * {@linkplain io.spine.client.OrderBy order clause}.
//     *
//     * @param ids
//     *         entity identifiers which are translated to Datastore keys
//     * @param fieldMask
//     *         a field mask specifying fields to be included in resulting entities
//     * @param predicate
//     *         a predicate which must be matched by entities to be returned as results
//     * @param orderBy
//     *         a specification of order in which the query results must be returned
//     * @return an iterator over the matching entity records
//     */
//    Iterator<EntityRecord> find(Iterable<I> ids,
//                                FieldMask fieldMask,
//                                Predicate<Entity> predicate,
//                                OrderBy orderBy) {
//        Iterator<EntityRecord> recordIterator =
//                findSortedRecords(ids, fieldMask, predicate, orderBy)
//                        .iterator();
//        return recordIterator;
//    }

//    private Stream<EntityRecord> findSortedRecords(Iterable<I> ids, FieldMask fieldMask,
//                                                   Predicate<Entity> predicate, OrderBy orderBy) {
//        Stream<EntityRecord> records =
//                readFiltered(ids, predicate)
//                        .sorted(implementing(orderBy))
//                        .map(toMaskedRecord(fieldMask));
//        return records;
//    }

//    /**
//     * Queries the records with supplied identifiers which match the provided predicate,
//     * and applies a field mask to the query results.
//     *
//     * <p>The order of the results is not guaranteed.
//     *
//     * @param ids
//     *         entity identifiers which are translated to Datastore keys
//     * @param fieldMask
//     *         a field mask specifying fields to be included in resulting entities
//     * @param predicate
//     *         a predicate which must be matched by entities to be returned as results
//     * @return an iterator over the matching entity records
//     */
//    Iterator<EntityRecord> find(Iterable<I> ids,
//                                FieldMask fieldMask,
//                                Predicate<Entity> predicate) {
//        return readFiltered(ids, predicate)
//                .map(toMaskedRecord(fieldMask))
//                .iterator();
//    }
//
//    private Stream<Entity> readFiltered(Iterable<I> ids, Predicate<Entity> predicate) {
//        Stream<@Nullable Entity> entities = read(ids)
//                .filter(Objects::nonNull)
//                .filter(predicate);
//        return entities;
//    }

//    /**
//     * Queries the records with supplied identifiers and applies a field mask to the query results.
//     *
//     * <p>The results are returned in an order matching that of the provided IDs with nulls
//     * in place of missing and inactive entities.
//     *
//     * @param ids
//     *         entity identifiers which are translated to Datastore keys
//     * @param fieldMask
//     *         a field mask specifying fields to be included in resulting entities
//     * @return an iterator over the matching nullable entity records
//     */
//    Iterator<@Nullable EntityRecord> findActive(Iterable<I> ids, FieldMask fieldMask) {
//        Iterator<EntityRecord> recordIterator = readActiveRecords(ids)
//                .map(nullableRecordMasker(fieldMask))
//                .iterator();
//        return recordIterator;
//    }

//    /**
//     * Queries the records with supplied identifiers.
//     *
//     * <p>The results are returned in an order matching that of the provided IDs with nulls
//     * in place of missing and inactive entities.
//     *
//     * @param ids
//     *         entity identifiers which are translated to Datastore keys
//     * @return an iterator over the matching nullable entity records
//     */
//    Iterator<@Nullable EntityRecord> findActive(Iterable<I> ids) {
//        Iterator<EntityRecord> recordIterator = readActiveRecords(ids)
//                .iterator();
//        return recordIterator;
//    }

//    private Stream<EntityRecord> readActiveRecords(Iterable<I> ids) {
//        Stream<EntityRecord> records = read(ids)
//                .map(Entities::nullableToRecord)
//                .filter(Objects::nonNull)
//                .map(nullIfNot(EntityRecord::isActive));
//        return records;
//    }

//    private static <O> Function<O, @Nullable O> nullIfNot(Predicate<O> predicate) {
//        return obj -> predicate.test(checkNotNull(obj)) ? obj : null;
//    }
}
