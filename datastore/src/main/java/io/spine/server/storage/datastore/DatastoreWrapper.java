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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.Transaction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.spine.logging.Logging;
import io.spine.server.storage.datastore.tenant.Namespace;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Streams.stream;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

/**
 * Adapts {@link Datastore} API for being used for storages.
 */
@SuppressWarnings("ClassWithTooManyMethods")
public class DatastoreWrapper implements Logging {

    static final int MAX_ENTITIES_PER_WRITE_REQUEST = 500;

    private final NamespaceSupplier namespaceSupplier;
    private final Datastore datastore;

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
        this.namespaceSupplier = checkNotNull(supplier);
        this.datastore = checkNotNull(datastore);
    }

    /**
     * Shortcut method for calling the constructor.
     */
    static DatastoreWrapper wrap(Datastore datastore, NamespaceSupplier supplier) {
        return new DatastoreWrapper(datastore, supplier);
    }

    /**
     * Creates an instance of {@link com.google.cloud.datastore.Key} basing on the Datastore
     * entity {@code kind} and {@code recordId}.
     *
     * @param kind
     *         the kind of the Datastore entity
     * @param recordId
     *         the ID of the record
     * @return the Datastore {@code Key} instance
     */
    Key keyFor(Kind kind, RecordId recordId) {
        KeyFactory keyFactory = keyFactory(kind);
        Key key = keyFactory.newKey(recordId.getValue());

        return key;
    }

    /**
     * Writes new {@link Entity} into the Datastore.
     *
     * @param entity
     *         new {@link Entity} to put into the Datastore
     * @throws DatastoreException
     *         upon failure
     * @see DatastoreWriter#add(FullEntity)
     */
    public void create(Entity entity) throws DatastoreException {
        datastore.add(entity);
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
        datastore.update(entity);
    }

    /**
     * Writes an {@link Entity} to the Datastore or modifies an existing one.
     *
     * @param entity
     *         the {@link Entity} to write or update
     * @see DatastoreWrapper#create(Entity)
     * @see DatastoreWrapper#update(Entity)
     */
    public void createOrUpdate(Entity entity) {
        datastore.put(entity);
    }

    /**
     * Writes the {@link Entity entities} to the Datastore or modifies the existing ones.
     *
     * @param entities
     *         the {@link Entity Entities} to write or update
     * @see DatastoreWrapper#createOrUpdate(Entity)
     */
    public void createOrUpdate(Entity... entities) {
        if (entities.length <= MAX_ENTITIES_PER_WRITE_REQUEST) {
            writeSmallBulk(entities);
        } else {
            writeBulk(entities);
        }
    }

    /**
     * Writes the {@link Entity entities} to the Datastore or modifies the existing ones.
     *
     * @param entities
     *         a {@link Collection} of {@link Entity Entities} to write or update
     * @see DatastoreWrapper#createOrUpdate(Entity)
     */
    public void createOrUpdate(Collection<Entity> entities) {
        Entity[] array = new Entity[entities.size()];
        entities.toArray(array);
        createOrUpdate(array);
    }

    /**
     * Retrieves an {@link Entity} with the given key from the Datastore.
     *
     * @param key
     *         {@link Key} to search for
     * @return the {@link Entity} or {@code null} in case of no results for the key given
     * @see DatastoreReader#get(Key)
     */
    public @Nullable Entity read(Key key) {
        return datastore.get(key);
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
     * @deprecated Use {@link #lookup(Collection)} instead.
     */
    @Deprecated
    public Iterator<@Nullable Entity> read(Iterable<Key> keys) {
        return lookup(ImmutableList.copyOf(keys)).iterator();
    }

    /**
     * Retrieves an {@link Entity} for each of the given keys.
     *
     * <p>The results are returned in an order matching that of the provided keys
     * with {@code null}s in place of missing and inactive entities.
     *
     * @param keys
     *         {@link Key Keys} to search for
     * @return an {@code List} of the found entities in the order of keys (including {@code null}
     *         values for nonexistent keys)
     * @see DatastoreReader#fetch(Key...)
     */
    public List<@Nullable Entity> lookup(Collection<Key> keys) {
        checkNotNull(keys);
        DsReaderLookup lookup = new DsReaderLookup(datastore);
        return lookup.find(keys);
    }

    /**
     * Queries the Datastore with the given arguments.
     *
     * <p>The Datastore may return a partial result set, so an execution of this method may
     * result in several Datastore queries.
     *
     * <p>The limit included in the {@link StructuredQuery}, will be a maximum count of objects
     * in the returned iterator.
     *
     * <p>The returned {@link DsQueryIterator} allows to {@linkplain DsQueryIterator#nextPageQuery()
     * create a query} to the next page of results reusing an existing cursor.
     *
     * <p>The resulting {@code Iterator} is evaluated lazily. A call to
     * {@link Iterator#remove() Iterator.remove()} causes an {@link UnsupportedOperationException}.
     *
     * @param query
     *         {@link Query} to execute upon the Datastore
     * @param <R>
     *         the type of queried objects
     * @return results fo the query as a lazily evaluated {@link Iterator}
     * @see DatastoreReader#run(Query)
     */
    public <R> DsQueryIterator<R> read(StructuredQuery<R> query) {
        DsReaderLookup lookup = new DsReaderLookup(datastore);
        return lookup.execute(query, namespaceSupplier.get());
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
    private <R> Iterator<R>
    readAllPageByPage(StructuredQuery<R> query, @Nullable Integer pageSize) {
        checkArgument(query.getLimit() == null,
                      "Cannot limit a number of entities for \"read all\" operation.");
        checkArgument(pageSize == null || pageSize != 0,
                      "The size of a single read operation cannot be 0.");

        StructuredQuery<R> limitedQuery = limit(query, pageSize);
        return stream(new DsQueryPageIterator<>(limitedQuery, this))
                .flatMap(Streams::stream)
                .iterator();
    }

    private static <R> StructuredQuery<R> limit(StructuredQuery<R> query,
                                                @Nullable Integer batchSize) {
        return batchSize == null
               ? query
               : query.toBuilder()
                      .setLimit(batchSize)
                      .build();
    }

    /**
     * Deletes all existing {@link Entity Entities} with the given keys.
     *
     * @param keys
     *         {@code Keys} of the {@code Entities} to delete. May be nonexistent
     */
    public void delete(Key... keys) {
        datastore.delete(keys);
    }

    /**
     * Deletes all existing {@link Entities} of a kind given.
     *
     * @param table
     *         kind (a.k.a. type, table, etc.) of the records to delete
     */
    @VisibleForTesting
    protected void dropTable(Kind table) {
        Namespace namespace = namespaceSupplier.get();
        StructuredQuery<Entity> query =
                Query.newEntityQueryBuilder()
                     .setNamespace(namespace.value())
                     .setKind(table.value())
                     .build();
        _trace().log("Deleting all entities of `%s` kind in `%s` namespace.",
                     table, namespace.value());
        Iterator<Entity> queryResult = read(query);
        List<Entity> entities = newArrayList(queryResult);
        deleteEntities(entities);
    }

    @VisibleForTesting
    protected void deleteEntities(Collection<Entity> entities) {
        List<Key> keyList =
                entities.stream()
                        .map(BaseEntity::getKey)
                        .collect(toList());
        Key[] keys = new Key[keyList.size()];
        keyList.toArray(keys);
        deleteEntities(keys);
    }

    private void deleteEntities(Key[] keys) {
        if (keys.length > MAX_ENTITIES_PER_WRITE_REQUEST) {
            int start = 0;
            int end = MAX_ENTITIES_PER_WRITE_REQUEST;
            while (true) {
                int length = end - start;
                if (length <= 0) {
                    return;
                }
                Key[] keysSubarray = new Key[length];
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
        Transaction tx = datastore.newTransaction();
        return new TransactionWrapper(tx, namespaceSupplier);
    }

    /**
     * Retrieves an instance of {@link KeyFactory} unique for given Kind of data
     * regarding the current namespace.
     *
     * @param kind
     *         kind of {@link Entity} to generate keys for
     * @return an instance of {@link KeyFactory} for given kind
     */
    public KeyFactory keyFactory(Kind kind) {
        checkNotNull(kind);
        KeyFactory keyFactory = datastore.newKeyFactory()
                                         .setKind(kind.value());
        Namespace namespace = namespaceSupplier.get();
        _trace().log("Retrieving KeyFactory for kind `%s` in `%s` namespace.",
                     kind, namespace.value());
        keyFactory.setNamespace(namespace.value());
        return keyFactory;
    }

    @VisibleForTesting
    public Datastore datastore() {
        return datastore;
    }

    private void writeBulk(Entity[] entities) {
        int partsCount = entities.length / MAX_ENTITIES_PER_WRITE_REQUEST + 1;
        for (int i = 0; i < partsCount; i++) {
            int partHead = i * MAX_ENTITIES_PER_WRITE_REQUEST;
            int partTail = min(partHead + MAX_ENTITIES_PER_WRITE_REQUEST, entities.length);

            Entity[] part = Arrays.copyOfRange(entities, partHead, partTail);
            writeSmallBulk(part);
        }
    }

    private void writeSmallBulk(Entity[] entities) {
        datastore.put(entities);
    }
}
