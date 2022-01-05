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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.InlineMe;
import io.spine.logging.Logging;
import io.spine.server.storage.datastore.record.Entities;
import io.spine.server.storage.datastore.record.RecordId;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Streams.stream;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

/**
 * Adapts {@link Datastore} API for being used for storages.
 */
public class DatastoreWrapper extends DatastoreMedium implements Logging {

    private static final int MAX_ENTITIES_PER_WRITE_REQUEST = 500;

    /**
     * Creates a new instance of {@code DatastoreWrapper}.
     *
     * @param datastore
     *         {@link Datastore} to wrap
     * @param supplier
     *         an instance of {@link Supplier Supplier&lt;Namespace&gt;} to get the namespaces for
     *         the queries from the datastore
     */
    protected DatastoreWrapper(Datastore datastore, NamespaceSupplier supplier) {
        super(datastore, supplier);
    }

    /**
     * Shortcut method for calling the constructor.
     */
    static DatastoreWrapper wrap(Datastore datastore, NamespaceSupplier supplier) {
        return new DatastoreWrapper(datastore, supplier);
    }

    @Override
    public Key keyFor(Kind kind, RecordId recordId) {
        var keyFactory = keyFactory(kind);
        var key = keyFactory.newKey(recordId.value());
        return key;
    }

    @Override
    public void create(Entity entity) throws DatastoreException {
        storage().add(entity);
    }

    /**
     * Modifies an {@link Entity} in the Datastore.
     *
     * @param entity
     *         the {@link Entity} to update
     * @throws DatastoreException
     *         if the {@link Entity} with such {@link Key} does not exist
     * @see DatastoreWriter#update(Entity...)
     */
    public void update(Entity entity) throws DatastoreException {
        storage().update(entity);
    }

    @Override
    public void createOrUpdate(Entity entity) {
        storage().put(entity);
    }

    @Override
    public void createOrUpdate(Collection<Entity> entities) {
        var array = new Entity[entities.size()];
        entities.toArray(array);
        if (array.length <= MAX_ENTITIES_PER_WRITE_REQUEST) {
            writeSmallBulk(array);
        } else {
            writeBulk(array);
        }
    }

    @Override
    public Optional<Entity> read(Key key) {
        return Optional.ofNullable(storage().get(key));
    }

    /**
     * Retrieves an {@link Entity} for each of the given keys.
     *
     * <p>The resulting {@code Iterator} is evaluated eagerly. A call to
     * {@link Iterator#remove() Iterator.remove()} causes an {@link UnsupportedOperationException}.
     *
     * <p>The results are returned in an order matching that of the provided keys
     * with {@code null}s in place of missing and inactive entities.
     *
     * @param keys
     *         {@link Key Keys} to search for
     * @return an {@code Iterator} over the found entities in the order of keys
     *         (including {@code null} values for nonexistent keys)
     * @see DatastoreReader#fetch(Key...)
     * @deprecated Use {@link #lookup(List)} instead.
     */
    @Deprecated
    @InlineMe(
            replacement = "this.lookup(ImmutableList.copyOf(keys)).iterator()",
            imports = "com.google.common.collect.ImmutableList"
    )
    public final Iterator<@Nullable Entity> read(Iterable<Key> keys) {
        return lookup(ImmutableList.copyOf(keys)).iterator();
    }

    @Override
    public List<@Nullable Entity> lookup(List<Key> keys) {
        checkNotNull(keys);
        var lookup = new DsReaderLookup(storage());
        return lookup.find(keys);
    }

    @Override
    public <R> DsQueryIterator<R> read(StructuredQuery<R> query) {
        var lookup = new DsReaderLookup(storage());
        return lookup.execute(query, namespace());
    }

    /**
     * Queries the Datastore for all entities matching query.
     *
     * <p>Read is performed from datastore using batches of the specified size, which leads to
     * multiple queries being executed.
     *
     * <p>The resulting {@code Iterator} is evaluated lazily. A call to
     * {@link Iterator#remove() Iterator.remove()} causes an {@link UnsupportedOperationException}.
     *
     * @param query
     *         {@link Query} to execute upon the Datastore
     * @param pageSize
     *         a non-zero number of elements to be returned per a single read from Datastore
     * @param <R>
     *         the type of queried objects
     * @return results fo the query as a lazily evaluated {@link Iterator}
     * @throws IllegalArgumentException
     *         if the provided {@linkplain StructuredQuery#getLimit() query includes a limit}
     */
    public <R> Iterator<R> readAll(StructuredQuery<R> query, int pageSize) {
        return readAllPageByPage(query, pageSize);
    }

    /**
     * Queries the Datastore for all entities matching query.
     *
     * <p>Read is performed in batches until all of the matching entities are fetched, resulting
     * in multiple Datastore queries.
     *
     * <p>The resulting {@code Iterator} is evaluated lazily. A call to
     * {@link Iterator#remove() Iterator.remove()} causes an {@link UnsupportedOperationException}.
     *
     * @param query
     *         {@link Query} to execute upon the Datastore
     * @param <R>
     *         the type of queried objects
     * @return results fo the query as a lazily evaluated {@link Iterator}
     * @throws IllegalArgumentException
     *         if the provided {@linkplain StructuredQuery#getLimit() query includes a limit}
     */
    @SuppressWarnings("unused")
    public <R> Iterator<R> readAll(StructuredQuery<R> query) {
        return readAllPageByPage(query, null);
    }

    /**
     * Queries the Datastore for all entities matching query, executing queries split in batches.
     *
     * <p>Read is performed from datastore using batches of the specified size, which leads to
     * multiple queries being executed.
     *
     * <p>The resulting {@code Iterator} is evaluated lazily. A call to
     * {@link Iterator#remove() Iterator.remove()} causes an {@link UnsupportedOperationException}.
     *
     * @param query
     *         a {@link Query} to execute upon the Datastore
     * @param pageSize
     *         a non-zero number of elements to be returned per a single read from Datastore;
     *         if {@code null} the page size will be dictated by the Datastore
     * @param <R>
     *         the type of queried objects
     * @return results fo the query as a lazily evaluated {@link Iterator}
     * @throws IllegalArgumentException
     *         if the provided {@linkplain StructuredQuery#getLimit() query includes a limit} or
     *         the provided {@code batchSize} is 0
     */
    @SuppressWarnings("UnstableApiUsage")   /* Guava's `Streams.stream` is fine. */
    private <R> Iterator<R>
    readAllPageByPage(StructuredQuery<R> query, @Nullable Integer pageSize) {
        checkArgument(query.getLimit() == null,
                      "Cannot limit a number of entities for \"read all\" operation.");
        checkArgument(pageSize == null || pageSize != 0,
                      "The size of a single read operation cannot be 0.");

        var limitedQuery = limit(query, pageSize);
        return stream(new DsQueryPageIterator<>(limitedQuery, this))
                .flatMap(Streams::stream)
                .iterator();
    }

    private static <R>
    StructuredQuery<R> limit(StructuredQuery<R> query, @Nullable Integer batchSize) {
        return batchSize == null
               ? query
               : query.toBuilder()
                      .setLimit(batchSize)
                      .build();
    }

    @Override
    public void delete(Key... keys) {
        storage().delete(keys);
    }

    /**
     * Deletes all existing {@link Entities} of a kind given.
     *
     * @param table
     *         kind (a.k.a. type, table, etc.) of the records to delete
     */
    @VisibleForTesting
    protected void dropTable(Kind table) {
        var namespace = namespace();
        var query = Query.newKeyQueryBuilder()
                         .setNamespace(namespace.value())
                         .setKind(table.value())
                         .build();
        _trace().log("Deleting all entities of `%s` kind in `%s` namespace.",
                     table, namespace.value());
        var queryResult = read(query);
        var keys = toIterable(queryResult);
        deleteEntities(toArray(keys, Key.class));
    }

    private static <T> Iterable<T> toIterable(Iterator<T> iterator) {
        return () -> iterator;
    }

    @VisibleForTesting
    protected void deleteEntities(Collection<Entity> entities) {
        var keyList = entities.stream()
                .map(BaseEntity::getKey)
                .collect(toList());
        var keys = new Key[keyList.size()];
        keyList.toArray(keys);
        deleteEntities(keys);
    }

    private void deleteEntities(Key[] keys) {
        if (keys.length > MAX_ENTITIES_PER_WRITE_REQUEST) {
            var start = 0;
            var end = MAX_ENTITIES_PER_WRITE_REQUEST;
            while (true) {
                var length = end - start;
                if (length <= 0) {
                    return;
                }
                var keysSubarray = new Key[length];
                System.arraycopy(keys, start, keysSubarray, 0, keysSubarray.length);
                delete(keysSubarray);

                start = end;
                end = min(MAX_ENTITIES_PER_WRITE_REQUEST, keys.length - end);
            }
        } else {
            delete(keys);
        }
    }

    /**
     * Starts a new database transaction.
     *
     * @return the new transaction
     * @see TransactionWrapper
     */
    public final TransactionWrapper newTransaction() {
        var tx = datastore().newTransaction();
        return new TransactionWrapper(tx, namespaceSupplier());
    }

    @Override
    public KeyFactory keyFactory(Kind kind) {
        checkNotNull(kind);
        var keyFactory = datastore()
                .newKeyFactory()
                .setKind(kind.value());
        var namespace = namespace();
        _trace().log("Retrieving KeyFactory for kind `%s` in `%s` namespace.",
                     kind, namespace.value());
        keyFactory.setNamespace(namespace.value());
        return keyFactory;
    }

    @VisibleForTesting
    public Datastore datastore() {
        return (Datastore) storage();
    }

    private void writeBulk(Entity[] entities) {
        var partsCount = entities.length / MAX_ENTITIES_PER_WRITE_REQUEST + 1;
        for (var i = 0; i < partsCount; i++) {
            var partHead = i * MAX_ENTITIES_PER_WRITE_REQUEST;
            var partTail = min(partHead + MAX_ENTITIES_PER_WRITE_REQUEST, entities.length);

            var part = Arrays.copyOfRange(entities, partHead, partTail);
            writeSmallBulk(part);
        }
    }

    private void writeSmallBulk(Entity[] entities) {
        storage().put(entities);
    }
}
