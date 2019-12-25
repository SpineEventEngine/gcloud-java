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

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Datastore;
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
import com.google.cloud.datastore.Transaction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Streams;
import io.spine.logging.Logging;
import io.spine.server.storage.datastore.tenant.Namespace;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.unmodifiableIterator;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Streams.stream;
import static java.lang.Math.min;
import static java.util.Collections.emptyIterator;
import static java.util.stream.Collectors.toList;

/**
 * Adapts {@link Datastore} API for being used for storages.
 */
@SuppressWarnings("ClassWithTooManyMethods")
public class DatastoreWrapper implements Logging {

    private static final String ACTIVE_TRANSACTION_CONDITION_MESSAGE =
            "Transaction should be active.";
    private static final String NOT_ACTIVE_TRANSACTION_CONDITION_MESSAGE =
            "Transaction should NOT be active.";

    private static final int MAX_KEYS_PER_READ_REQUEST = 1000;
    static final int MAX_ENTITIES_PER_WRITE_REQUEST = 500;

    private static final Map<DatastoreKind, KeyFactory> keyFactories = new HashMap<>();

    private static final Key[] EMPTY_KEY_ARRAY = new Key[0];

    private final NamespaceSupplier namespaceSupplier;
    private final Datastore datastore;
    private Transaction activeTransaction;
    private DatastoreReaderWriter actor;

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
        this.actor = datastore;
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
     * @see DatastoreWriter#put(FullEntity)
     */
    public void create(Entity entity) throws DatastoreException {
        actor.add(entity);
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
        actor.update(entity);
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
        actor.put(entity);
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
        return actor.get(key);
    }

    /**
     * Retrieves an {@link Entity} for each of the given keys.
     *
     * <p>The resulting {@code Iterator} is evaluated lazily. A call to
     * {@link Iterator#remove() Iterator.remove()} causes an {@link UnsupportedOperationException}.
     *
     * <p>The results are returned in an order matching that of the provided keys
     * with {@code null}s in place of missing and inactive entities.
     *
     * @param keys
     *         {@link Key Keys} to search for
     * @return an {@code Iterator} over the found entities in the order of keys
     *         (including {@code null} values for nonexistent keys)
     * @see DatastoreReader#get(Key...)
     */
    public Iterator<@Nullable Entity> read(Iterable<Key> keys) {
        Iterator<@Nullable Entity> dsIterator = readByKeys(keys);
        Iterator<@Nullable Entity> result = orderByKeys(keys, dsIterator);
        return unmodifiableIterator(result);
    }

    private Iterator<@Nullable Entity> readByKeys(Iterable<Key> keys) {
        List<Key> keysList = newLinkedList(keys);
        return keysList.size() <= MAX_KEYS_PER_READ_REQUEST
               ? actor.get(toArray(keys, Key.class))
               : readBulk(keysList);
    }

    private static Iterator<@Nullable Entity> orderByKeys(Iterable<Key> keys,
                                                          Iterator<Entity> items) {
        List<Entity> entities = newLinkedList(() -> items);
        Iterator<Entity> entitiesIterator = stream(keys)
                .map(key -> getEntityOrNull(key, entities.iterator()))
                .iterator();
        return entitiesIterator;
    }

    private static @Nullable Entity getEntityOrNull(Key key, Iterator<Entity> entities) {
        while (entities.hasNext()) {
            Entity entity = entities.next();
            if (key.equals(entity.getKey())) {
                entities.remove();
                return entity;
            }
        }
        return null;
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
        Namespace namespace = namespaceSupplier.get();
        StructuredQuery<R> queryWithNamespace =
                query.toBuilder()
                     .setNamespace(namespace.getValue())
                     .build();
        _trace().log("Reading entities of `%s` kind in `%s` namespace.",
                     query.getKind(), query.getFilter(), namespace.getValue());
        DsQueryIterator<R> result = new DsQueryIterator<>(queryWithNamespace, actor);
        return result;
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
    @SuppressWarnings("unchecked") // Checked logically.
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
     *         {@link Key Keys} of the {@link Entity Entities} to delete. May be nonexistent
     */
    public void delete(Key... keys) {
        actor.delete(keys);
    }

    /**
     * Deletes all existing {@link Entities} of a kind given.
     *
     * @param table
     *         kind (a.k.a. type, table, etc.) of the records to delete
     */
    @VisibleForTesting
    protected void dropTable(String table) {
        Namespace namespace = namespaceSupplier.get();
        StructuredQuery<Entity> query =
                Query.newEntityQueryBuilder()
                     .setNamespace(namespace.getValue())
                     .setKind(table)
                     .build();
        _trace().log("Deleting all entities of `%s` kind in `%s` namespace.",
                     table, namespace.getValue());
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
        return new TransactionWrapper(tx);
    }

    /**
     * Starts a transaction.
     *
     * <p>After this method is called, all {@code Entity} modifications performed through this
     * instance of {@code DatastoreWrapper} become transactional. This behaviour lasts until either
     * {@link #commitTransaction()} or {@link #rollbackTransaction()} is called.
     *
     * @throws IllegalStateException
     *         if a transaction is already started on this instance of
     *         {@code DatastoreWrapper}
     * @see #isTransactionActive()
     * @deprecated Use {@link #newTransaction()} instead.
     */
    @Deprecated
    public void startTransaction() throws IllegalStateException {
        checkState(!isTransactionActive(), NOT_ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction = datastore.newTransaction();
        actor = activeTransaction;
    }

    /**
     * Commits a transaction.
     *
     * <p>Upon the method call, all the modifications within the active transaction are applied.
     *
     * <p>All next operations become non-transactional until {@link #startTransaction()} is called.
     *
     * @throws IllegalStateException
     *         if no transaction is started on this instance of
     *         {@code DatastoreWrapper}
     * @see #isTransactionActive()
     * @deprecated Use {@link #newTransaction()} instead.
     */
    @Deprecated
    public void commitTransaction() throws IllegalStateException {
        checkState(isTransactionActive(), ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction.commit();
        this.actor = datastore;
    }

    /**
     * Rollbacks a transaction.
     *
     * <p>Upon the method call, all the modifications within the active transaction
     * canceled permanently.
     *
     * <p>After this method execution is over, all the further modifications made through
     * the current instance of {@code DatastoreWrapper} become non-transactional.
     *
     * @throws IllegalStateException
     *         if no transaction is active for the current
     *         instance of {@code DatastoreWrapper}
     * @see #isTransactionActive()
     * @deprecated Use {@link #newTransaction()} instead.
     */
    @Deprecated
    public void rollbackTransaction() throws IllegalStateException {
        checkState(isTransactionActive(), ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction.rollback();
        this.actor = datastore;
    }

    /**
     * Checks whether there is an active transaction on this instance of {@code DatastoreWrapper}.
     *
     * @return {@code true} if there is an active transaction, {@code false} otherwise
     * @deprecated Use {@link #newTransaction()} instead.
     */
    @Deprecated
    public boolean isTransactionActive() {
        return activeTransaction != null && activeTransaction.isActive();
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
        DatastoreKind datastoreKind = new DatastoreKind(projectId(), kind);
        KeyFactory keyFactory = keyFactories.get(datastoreKind);
        if (keyFactory == null) {
            keyFactory = initKeyFactory(kind);
        }
        Namespace namespace = namespaceSupplier.get();
        _trace().log("Retrieving KeyFactory for kind `%s` in `%s` namespace.",
                     kind, namespace.getValue());
        keyFactory.setNamespace(namespace.getValue());
        return keyFactory;
    }

    @VisibleForTesting
    public Datastore datastore() {
        return datastore;
    }

    private KeyFactory initKeyFactory(Kind kind) {
        KeyFactory keyFactory = datastore.newKeyFactory()
                                         .setKind(kind.value());
        DatastoreKind datastoreKind = new DatastoreKind(projectId(), kind);
        keyFactories.put(datastoreKind, keyFactory);
        return keyFactory;
    }

    private ProjectId projectId() {
        String projectId = datastore.getOptions()
                                    .getProjectId();
        ProjectId result = ProjectId.of(projectId);
        return result;
    }

    /**
     * Reads big number of records.
     *
     * <p>Google App Engine Datastore has a limitation on the amount of entities queried with a
     * single call — 1000 entities per query. To deal with this limitation we read the entities in
     * pagination fashion 1000 entity per page.
     *
     * @param keys
     *         {@link Key keys} to find the entities for
     * @return ordered sequence of {@link Entity entities}
     * @see #read(Iterable)
     */
    private Iterator<Entity> readBulk(List<Key> keys) {
        int pageCount = keys.size() / MAX_KEYS_PER_READ_REQUEST + 1;
        _trace().log("Reading a big bulk of entities synchronously. The data is read as %d pages.",
                     pageCount);
        int lowerBound = 0;
        int higherBound = MAX_KEYS_PER_READ_REQUEST;
        int keysLeft = keys.size();
        Iterator<Entity> result = emptyIterator();
        for (int i = 0; i < pageCount; i++) {
            List<Key> keysPage = keys.subList(lowerBound, higherBound);

            Iterator<Entity> page = actor.get(keysPage.toArray(EMPTY_KEY_ARRAY));
            result = concat(result, page);

            keysLeft -= keysPage.size();
            lowerBound = higherBound;
            higherBound += min(keysLeft, MAX_KEYS_PER_READ_REQUEST);
        }

        return result;
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
        actor.put(entities);
    }

    /**
     * A Datastore {@link Kind} by project ID.
     */
    private static class DatastoreKind {

        private final ProjectId projectId;
        private final Kind kind;

        private DatastoreKind(ProjectId projectId, Kind kind) {
            this.projectId = projectId;
            this.kind = kind;
        }

        @SuppressWarnings("EqualsGetClass") // The class is effectively final.
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DatastoreKind kind1 = (DatastoreKind) o;
            return Objects.equals(projectId, kind1.projectId) &&
                    Objects.equals(kind, kind1.kind);
        }

        @Override
        public int hashCode() {
            return Objects.hash(projectId, kind);
        }
    }
}
