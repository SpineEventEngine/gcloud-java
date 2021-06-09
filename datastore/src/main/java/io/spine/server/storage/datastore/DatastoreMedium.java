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
import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import io.spine.server.storage.datastore.record.RecordId;
import io.spine.server.storage.datastore.tenant.Namespace;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A common interface for those who wrap {@link DatastoreReaderWriter}s and provide their own
 * API facade on top of them.
 */
public abstract class DatastoreMedium {

    private final DatastoreReaderWriter datastoreReaderWriter;
    private final NamespaceSupplier namespaceSupplier;

    /**
     * Creates a new instance of this type wrapping the low-level reader-writer for Datastore and
     * the provider of the current Datastore namespace.
     */
    protected DatastoreMedium(DatastoreReaderWriter writer, NamespaceSupplier supplier) {
        datastoreReaderWriter = checkNotNull(writer);
        namespaceSupplier = checkNotNull(supplier);
    }

    /**
     * Returns the low-level reader-writer implementation.
     */
    protected final DatastoreReaderWriter storage() {
        return datastoreReaderWriter;
    }

    /**
     * Returns the supplier of current Datastore namespace basing on the ID of the current Tenant.
     */
    protected final NamespaceSupplier namespaceSupplier() {
        return namespaceSupplier;
    }

    /**
     * Creates a new {@link Entity} in the Datastore.
     *
     * @param entity
     *         new {@link Entity} to create
     * @throws DatastoreException
     *         upon failure
     * @see DatastoreWriter#add(FullEntity)
     */
    public abstract void create(Entity entity) throws DatastoreException;

    /**
     * Writes an {@link Entity} to the Datastore or modifies an existing one.
     *
     * @param entity
     *         the {@link Entity} to write or update
     * @see DatastoreWrapper#create(Entity)
     * @see DatastoreWrapper#update(Entity)
     */
    public abstract void createOrUpdate(Entity entity) throws DatastoreException;

    /**
     * Writes the {@link Entity entities} to the Datastore or modifies the existing ones.
     *
     * @param entities
     *         a {@link Collection} of {@link Entity Entities} to write or update
     * @see DatastoreWrapper#createOrUpdate(Entity)
     */
    public abstract void createOrUpdate(Collection<Entity> entities) throws DatastoreException;

    /**
     * Retrieves an {@link Entity} with the given key from the Datastore.
     *
     * @param key
     *         {@link Key} to search for
     * @return the {@link Entity} or {@code null} in case of no results for the key given
     * @see DatastoreReader#get(Key)
     */
    public abstract Optional<Entity> read(Key key);

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
    public abstract List<Entity> lookup(List<Key> keys);

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
    public abstract <R> DsQueryIterator<R> read(StructuredQuery<R> query) throws DatastoreException;

    /**
     * Deletes all existing Datastore Entities with the passed keys.
     *
     * <p>Keys may not correspond to the existing Entities. In this case, they are ignored.
     */
    public abstract void delete(Key... keys);

    /**
     * Returns the Datastore namespace which corresponds to the current Tenant.
     *
     * @see NamespaceSupplier
     */
    protected Namespace namespace() {
        return namespaceSupplier().get();
    }

    /**
     * Creates an instance of {@link Key} basing on the kind and identifier of Datastore Entity.
     *
     * @param kind
     *         the kind of the Datastore entity
     * @param id
     *         the ID of the record
     * @return the Datastore {@code Key} instance
     */
    public abstract Key keyFor(Kind kind, RecordId id);

    /**
     * Retrieves an instance of {@link KeyFactory} unique for given Kind of data
     * regarding the current namespace.
     *
     * @param kind
     *         kind of Entity to generate keys for
     * @return an instance of {@link KeyFactory} for given kind
     */
    public abstract KeyFactory keyFactory(Kind kind);
}
