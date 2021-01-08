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

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.Transaction;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.ofNullable;

/**
 * A Cloud Datastore transaction wrapper.
 */
public final class TransactionWrapper implements AutoCloseable {

    private final Transaction tx;
    private final NamespaceSupplier namespaceSupplier;

    TransactionWrapper(Transaction tx,
                       NamespaceSupplier namespaceSupplier) {
        this.tx = checkNotNull(tx);
        this.namespaceSupplier = checkNotNull(namespaceSupplier);
    }

    /**
     * Creates a new entity in the Datastore in the transaction.
     *
     * <p>Throws a {@link DatastoreException} if an entity with such a key already exists.
     *
     * @param entity
     *         new {@link Entity} to put into the Datastore
     * @throws DatastoreException
     *         upon failure
     * @see Transaction#add(com.google.cloud.datastore.FullEntity)
     */
    public void create(Entity entity) throws DatastoreException {
        tx.add(entity);
    }

    /**
     * Creates multiple new entities in the Datastore in the transaction.
     *
     * <p>Throws a {@link DatastoreException} if at least one entity key clashes with an existing
     * one.
     *
     * @param entities
     *         new entities to put into the Datastore
     * @throws DatastoreException
     *         upon failure
     * @see Transaction#add(com.google.cloud.datastore.FullEntity...)
     */
    public void create(Collection<Entity> entities) throws DatastoreException {
        Entity[] array = new Entity[entities.size()];
        entities.toArray(array);
        tx.add(array);
    }

    /**
     * Puts the given entity into the Datastore in the transaction.
     */
    public void createOrUpdate(Entity entity) throws DatastoreException {
        tx.put(entity);
    }

    /**
     * Puts the given entities into the Datastore in the transaction.
     *
     * @implNote Unlike {@link DatastoreWrapper}, {@code TransactionWrapper} does not provide
     *         a mechanism for writing large numbers of entities. Only 500 entities can be written
     *         in a single transaction. Please see
     *         the <a href="https://cloud.google.com/datastore/docs/concepts/limits">transaction
     *         limits</a> for more info.
     */
    public void createOrUpdate(Collection<Entity> entities) throws DatastoreException {
        Entity[] array = new Entity[entities.size()];
        entities.toArray(array);
        tx.put(array);
    }

    /**
     * Reads an entity from the Datastore in the transaction.
     *
     * @return the entity with the given key or {@code Optional.empty()} if such an entity does not
     *         exist
     */
    public Optional<Entity> read(Key key) {
        Entity entity = tx.get(key);
        return ofNullable(entity);
    }

    /**
     * Retrieves an {@link Entity} for each of the given keys.
     *
     * <p>The results are returned in an order matching that of the provided keys
     * with {@code null}s in place of missing and inactive entities.
     *
     * <p>In Datastore native mode, a transaction can only access 25 entity groups over its entire
     * lifespan. If the entities do not have ancestors, this translates to 25 entities per
     * transaction. See the <a href="https://cloud.google.com/datastore/docs/concepts/limits">
     * Datastore limits</a> for more info.
     *
     * @param keys
     *         {@link Key Keys} to search for
     * @return an {@code List} of the found entities in the order of keys (including {@code null}
     *         values for nonexistent keys)
     * @see com.google.cloud.datastore.DatastoreReader#fetch(Key...)
     */
    public List<Entity> lookup(Collection<Key> keys) {
        checkNotNull(keys);
        DsReaderLookup lookup = new DsReaderLookup(tx);
        return lookup.find(keys);
    }

    /**
     * Queries the Datastore with the given arguments within the transaction.
     *
     * <p>Datastore only supports ancestor queries within a transaction.
     * A {@link DatastoreException} is thrown if the given query is not an ancestor query.
     *
     * <p>The Datastore may return a partial result set, so an execution of this method may result
     * in several Datastore queries.
     *
     * <p>The limit included in the {@link StructuredQuery}, will be a maximum count of objects in
     * the returned iterator.
     *
     * <p>The returned {@link DsQueryIterator} allows to {@linkplain DsQueryIterator#nextPageQuery()
     * create a query} to the next page of results reusing an existing cursor.
     *
     * <p>The resulting {@code Iterator} is evaluated lazily. A call to
     * {@link Iterator#remove() Iterator.remove()} causes an {@link UnsupportedOperationException}.
     *
     * @param ancestorQuery
     *         {@link Query} to execute upon the Datastore
     * @param <R>
     *         the type of queried objects
     * @return results fo the query as a lazily evaluated {@link Iterator}
     */
    public <R> DsQueryIterator<R> read(StructuredQuery<R> ancestorQuery) throws DatastoreException {
        DsReaderLookup lookup = new DsReaderLookup(tx);
        return lookup.execute(ancestorQuery, namespaceSupplier.get());
    }

    /**
     * Deletes all existing {@link Entity Entities} with the given keys in a scope of
     * the ongoing transaction.
     *
     * @param keys
     *         the keys of the entities to delete; may point to non-existent entities
     */
    public void delete(Key... keys) {
        tx.delete(keys);
    }

    /**
     * Commits this transaction.
     *
     * @throws DatastoreException
     *         if the transaction is no longer active
     */
    public void commit() {
        tx.commit();
    }

    /**
     * Rolls back this transaction.
     *
     * @throws DatastoreException
     *         if the transaction is no longer active
     */
    public void rollback() {
        tx.rollback();
    }

    /**
     * Rolls back this transaction if it's still active.
     */
    @Override
    public void close() {
        if (tx.isActive()) {
            rollback();
        }
    }
}
