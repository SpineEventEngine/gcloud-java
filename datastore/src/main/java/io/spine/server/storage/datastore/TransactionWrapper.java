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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.Transaction;
import io.spine.server.storage.datastore.record.RecordId;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.ofNullable;

/**
 * A Cloud Datastore transaction wrapper.
 */
public final class TransactionWrapper extends DatastoreMedium implements AutoCloseable {


    TransactionWrapper(Transaction tx, NamespaceSupplier namespaceSupplier) {
        super(tx, namespaceSupplier);
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
    @Override
    public void create(Entity entity) throws DatastoreException {
        storage().add(entity);
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
        var array = new Entity[entities.size()];
        entities.toArray(array);
        storage().add(array);
    }

    /**
     * Puts the given entity into the Datastore in the transaction.
     */
    @Override
    public void createOrUpdate(Entity entity) throws DatastoreException {
        storage().put(entity);
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
    @Override
    public void createOrUpdate(Collection<Entity> entities) throws DatastoreException {
        var array = new Entity[entities.size()];
        entities.toArray(array);
        storage().put(array);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Executes the operation within this transaction.
     */
    @Override
    public Optional<Entity> read(Key key) {
        var entity = storage().get(key);
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
    @Override
    public List<Entity> lookup(List<Key> keys) {
        checkNotNull(keys);
        var lookup = new DsReaderLookup(storage());
        return lookup.find(keys);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Executes the operation within this transaction.
     */
    @Override
    public <R> DsQueryIterator<R> read(StructuredQuery<R> ancestorQuery) throws DatastoreException {
        var lookup = new DsReaderLookup(storage());
        return lookup.execute(ancestorQuery, namespace());
    }

    @Override
    public Key keyFor(Kind kind, RecordId id) {
        var result = wrapper().keyFor(kind, id);
        return result;
    }

    @Override
    public KeyFactory keyFactory(Kind kind) {
        var result = wrapper().keyFactory(kind);
        return result;
    }

    /**
     * Deletes all existing {@link Entity Entities} with the given keys in a scope of
     * the ongoing transaction.
     *
     * @param keys
     *         the keys of the entities to delete; may point to non-existent entities
     */
    @Override
    public void delete(Key... keys) {
        storage().delete(keys);
    }

    /**
     * Commits this transaction.
     *
     * @throws DatastoreException
     *         if the transaction is no longer active
     */
    public void commit() {
        tx().commit();
    }

    /**
     * Rolls back this transaction.
     *
     * @throws DatastoreException
     *         if the transaction is no longer active
     */
    public void rollback() {
        tx().rollback();
    }

    /**
     * Rolls back this transaction if it's still active.
     */
    @Override
    public void close() {
        if (tx().isActive()) {
            rollback();
        }
    }

    private Transaction tx() {
        return (Transaction) storage();
    }

    private DatastoreWrapper wrapper() {
        var naked = tx().getDatastore();
        var wrapper = DatastoreWrapper.wrap(naked, namespaceSupplier());
        return wrapper;
    }
}
