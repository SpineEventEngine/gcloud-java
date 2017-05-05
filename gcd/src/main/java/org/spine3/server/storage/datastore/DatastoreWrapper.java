/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.Cursor;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.ProjectionEntityQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.Transaction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.storage.datastore.tenant.DsNamespaceValidator;
import org.spine3.server.storage.datastore.tenant.Namespace;
import org.spine3.server.storage.datastore.tenant.NamespaceSupplier;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newLinkedList;
import static java.lang.Math.min;

/**
 * Represents a wrapper above GAE {@link Datastore}.
 *
 * <p>Provides API for Datastore to be used in storages.
 *
 * @author Dmytro Dashenkov
 */
public class DatastoreWrapper {

    private static final String ACTIVE_TRANSACTION_CONDITION_MESSAGE = "Transaction should be active.";
    private static final String NOT_ACTIVE_TRANSACTION_CONDITION_MESSAGE = "Transaction should NOT be active.";

    private static final int MAX_KEYS_PER_READ_REQUEST = 1000;
    private static final int MAX_ENTITIES_PER_WRITE_REQUEST = 500;

    private static final Map<Kind, KeyFactory> keyFactories = new HashMap<>();

    private final NamespaceSupplier namespaceSupplier;
    private final Datastore datastore;
    private Transaction activeTransaction;
    private DatastoreReaderWriter actor;

    private DsNamespaceValidator namespaceValidator;

    /**
     * Creates a new instance of {@code DatastoreWrapper}.
     *
     * @param datastore         {@link Datastore} to wrap
     * @param namespaceSupplier the instance of {@link Supplier Namespace Supplier}, providing
     *                          the namespaces for Datastore queries
     */
    protected DatastoreWrapper(Datastore datastore, NamespaceSupplier namespaceSupplier) {
        this.namespaceSupplier = checkNotNull(namespaceSupplier);
        this.datastore = checkNotNull(datastore);
        this.actor = datastore;
    }

    /**
     * Wraps {@link Datastore} into an instance of {@code DatastoreWrapper} and returns
     * the instance.
     *
     * @param datastore         {@link Datastore} to wrap
     * @param namespaceSupplier an instance of {@link Supplier Supplier&lt;Namespace&gt;} to get the
     *                          namespaces for the queries from
     * @return new instance of {@code DatastoreWrapper}
     */
    @SuppressWarnings("WeakerAccess") // Part of API
    protected static DatastoreWrapper wrap(Datastore datastore,
                                           NamespaceSupplier namespaceSupplier) {
        return new DatastoreWrapper(datastore, namespaceSupplier);
    }

    /**
     * Writes new {@link Entity} into the Datastore.
     *
     * @param entity new {@link Entity} to put into the Datastore
     * @throws DatastoreException upon failure
     * @see DatastoreWriter#put(FullEntity)
     */
    @SuppressWarnings("WeakerAccess")
    public void create(Entity entity) throws DatastoreException {
        actor.add(entity);
    }

    /**
     * Modifies an {@link Entity} in the Datastore.
     *
     * @param entity the {@link Entity} to update
     * @throws DatastoreException if the {@link Entity} with such {@link Key} does not exist
     * @see DatastoreWriter#update(Entity...)
     */
    @SuppressWarnings("WeakerAccess")
    public void update(Entity entity) throws DatastoreException {
        actor.update(entity);
    }

    /**
     * Writes an {@link Entity} to the Datastore or modifies an existing one.
     *
     * @param entity the {@link Entity} to write or update
     * @see DatastoreWrapper#create(Entity)
     * @see DatastoreWrapper#update(Entity)
     */
    public void createOrUpdate(Entity entity) {
        actor.put(entity);
    }

    /**
     * Writes the {@link Entity entities} to the Datastore or modifies the existing ones.
     *
     * @param entities the {@link Entity Entities} to write or update
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
     * @param entities a {@link Collection} of {@link Entity Entities} to write or update
     * @see DatastoreWrapper#createOrUpdate(Entity)
     */
    public void createOrUpdate(Collection<Entity> entities) {
        final Entity[] array = new Entity[entities.size()];
        entities.toArray(array);
        createOrUpdate(array);
    }

    /**
     * Retrieves an {@link Entity} with the given key from the Datastore.
     *
     * @param key {@link Key} to search for
     * @return the {@link Entity} or {@code null} in case of no results for the key given
     * @see DatastoreReader#get(Key)
     */
    public Entity read(Key key) {
        return actor.get(key);
    }

    /**
     * Retrieves an {@link Entity} for each of the given keys.
     *
     * @param keys {@link Key Keys} to search for
     * @return A list of found entities in the order of keys (including {@code null} values for
     * nonexistent keys)
     * @see DatastoreReader#fetch(Key...)
     */
    public List<Entity> read(Iterable<Key> keys) {
        final List<Key> keysList = newLinkedList(keys);
        final List<Entity> result;
        if (keysList.size() <= MAX_KEYS_PER_READ_REQUEST) {
            result = Lists.newArrayList(datastore.get(keys));
        } else {
            result = readBulk(keysList);
        }
        return result;
    }

    /**
     * Queries the Datastore with the given arguments.
     *
     * <p>As the Datastore may return a partial result set for {@link EntityQuery},
     * {@link KeyQuery} and {@link ProjectionEntityQuery}, it is required to repeat a query with
     * the adjusted cursor position.
     *
     * <p>Therefore, an execution of this method may in fact result in several queries to
     * the Datastore instance.
     *
     * @param query {@link Query} to execute upon the Datastore
     * @return results fo the query packed in a {@link List}
     * @see DatastoreReader#run(Query)
     */
    public List<Entity> read(StructuredQuery<Entity> query) {
        final Namespace namespace = getNamespace();
        final StructuredQuery<Entity> queryWithNamespace = query.toBuilder()
                                                                .setNamespace(namespace.getValue())
                                                                .build();
        QueryResults<Entity> queryResults = actor.run(queryWithNamespace);
        final List<Entity> resultsAsList = newLinkedList();

        while (queryResults != null && queryResults.hasNext()) {
            Iterators.addAll(resultsAsList, queryResults);

            final Cursor cursorAfter = queryResults.getCursorAfter();
            final Query<Entity> queryForMoreResults =
                    queryWithNamespace.toBuilder()
                                      .setStartCursor(cursorAfter)
                                      .build();

            queryResults = actor.run(queryForMoreResults);
        }

        return resultsAsList;
    }

    /**
     * Deletes all existing {@link Entity Entities} with the given keys.
     *
     * @param keys {@link Key Keys} of the {@link Entity Entities} to delete. May be nonexistent
     */
    public void delete(Key... keys) {
        actor.delete(keys);
    }

    /**
     * Deletes all existing {@link Entities} of a kind given.
     *
     * @param table kind (a.k.a. type, table, etc.) of the records to delete
     */
    void dropTable(String table) {
        final Namespace namespace = getNamespace();
        final StructuredQuery<Entity> query = Query.newEntityQueryBuilder()
                                                   .setNamespace(namespace.getValue())
                                                   .setKind(table)
                                                   .build();
        final List<Entity> entities = read(query);
        final Collection<Key> keys = Collections2.transform(entities, new Function<Entity, Key>() {
            @Nullable
            @Override
            public Key apply(@Nullable Entity input) {
                if (input == null) {
                    return null;
                }

                return input.getKey();
            }
        });

        final Key[] keysArray = new Key[keys.size()];
        keys.toArray(keysArray);
        dropTableInternal(keysArray);
    }

    void dropTableInternal(Key[] keysArray) {
        if (keysArray.length > MAX_ENTITIES_PER_WRITE_REQUEST) {
            int start = 0;
            int end = MAX_ENTITIES_PER_WRITE_REQUEST;
            while (true) {
                final int length = end - start;
                if (length <= 0) {
                    return;
                }
                final Key[] keysSubarray = new Key[length];
                System.arraycopy(keysArray, start, keysSubarray, 0, keysSubarray.length);
                delete(keysSubarray);

                start = end;
                end = min(MAX_ENTITIES_PER_WRITE_REQUEST, keysArray.length - end);
            }
        } else {
            delete(keysArray);
        }
    }

    /**
     * Starts a transaction.
     *
     * <p>After this method is called, all {@code Entity} modifications performed through this instance of
     * {@code DatastoreWrapper} become transactional. This behaviour lasts until either {@link #commitTransaction()} or
     * {@link #rollbackTransaction()} is called.
     *
     * @throws IllegalStateException if a transaction is already started on this instance of {@code DatastoreWrapper}
     * @see #isTransactionActive()
     */
    @SuppressWarnings("WeakerAccess") // Part of API
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
     * @throws IllegalStateException if no transaction is started on this instance of {@code DatastoreWrapper}
     * @see #isTransactionActive()
     */
    @SuppressWarnings("WeakerAccess")
    // Part of API
    public void commitTransaction() throws IllegalStateException {
        checkState(isTransactionActive(), ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction.commit();
        this.actor = datastore;
    }

    /**
     * Rollbacks a transaction.
     *
     * <p>Upon the method call, all the modifications within the active transaction canceled permanently.
     *
     * <p>After this method execution is over, all the further modifications made through the current instance of
     * {@code DatastoreWrapper} become non-transactional.
     *
     * @throws IllegalStateException if no transaction is active for the current instance of {@code DatastoreWrapper}
     * @see #isTransactionActive()
     */
    @SuppressWarnings("WeakerAccess") // Part of API
    public void rollbackTransaction() throws IllegalStateException {
        checkState(isTransactionActive(), ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction.rollback();
        this.actor = datastore;
    }

    /**
     * Checks whether there is an active transaction on this instance of {@code DatastoreWrapper}.
     *
     * @return {@code true} if there is an active transaction, {@code false} otherwise
     */
    @SuppressWarnings("WeakerAccess") // Part of API
    public boolean isTransactionActive() {
        return activeTransaction != null && activeTransaction.isActive();
    }

    /**
     * Retrieves an instance of {@link KeyFactory} unique for given Kind of data regarding the current namespace.
     *
     * @param kind kind of {@link Entity} to generate keys for
     * @return an instance of {@link KeyFactory} for given kind
     */
    public KeyFactory getKeyFactory(Kind kind) {
        KeyFactory keyFactory = keyFactories.get(kind);
        if (keyFactory == null) {
            keyFactory = initKeyFactory(kind);
        }
        final Namespace namespace = getNamespace();
        keyFactory.setNamespace(namespace.getValue());

        return keyFactory;
    }

    public DatastoreOptions getDatastoreOptions() {
        final DatastoreOptions options = datastore.getOptions()
                                                  .toBuilder()
                                                  .build();
        return options;
    }

    @VisibleForTesting
    Datastore getDatastore() {
        return datastore;
    }

    private KeyFactory initKeyFactory(Kind kind) {
        final KeyFactory keyFactory = datastore.newKeyFactory()
                                               .setKind(kind.getValue());
        keyFactories.put(kind, keyFactory);
        return keyFactory;
    }

    /**
     * Reads big number of records.
     *
     * <p>Google App Engine Datastore has a limitation on the amount of entities queried with a single call —
     * 1000 entities per query. To deal with this limitation we read the entities in pagination fashion
     * 1000 entity per page.
     *
     * @param keys {@link Key keys} to find the entities for
     * @return ordered sequence of {@link Entity entities}
     * @see #read(Iterable)
     */
    private List<Entity> readBulk(List<Key> keys) {
        final int pageCount = keys.size() / MAX_KEYS_PER_READ_REQUEST + 1;
        log().debug("Reading a big bulk of entities synchronously. The data is read as {} pages.", pageCount);

        final List<Entity> result = newLinkedList();
        int lowerBound = 0;
        int higherBound = MAX_KEYS_PER_READ_REQUEST;
        int keysLeft = keys.size();
        for (int i = 0; i < pageCount; i++) {
            final List<Key> keysPage = keys.subList(lowerBound, higherBound);

            final List<Entity> page = Lists.newArrayList(datastore.get(keysPage));
            result.addAll(page);

            keysLeft -= keysPage.size();
            lowerBound = higherBound;
            higherBound += min(keysLeft, MAX_KEYS_PER_READ_REQUEST);
        }

        return result;
    }

    private void writeBulk(Entity[] entities) {
        final int partsCount = entities.length / MAX_ENTITIES_PER_WRITE_REQUEST + 1;
        for (int i = 0; i < partsCount; i++) {
            final int partHead = i * MAX_ENTITIES_PER_WRITE_REQUEST;
            final int partTail = min(partHead + MAX_ENTITIES_PER_WRITE_REQUEST, entities.length);

            final Entity[] part = Arrays.copyOfRange(entities, partHead, partTail);
            writeSmallBulk(part);
        }
    }

    private Namespace getNamespace() {
        final Namespace namespace = namespaceSupplier.get();
        namespaceValidator().validate(namespace);
        return namespace;
    }

    private DsNamespaceValidator namespaceValidator() {
        if (namespaceValidator == null) {
            namespaceValidator = new DsNamespaceValidator(getDatastore(),
                                                          namespaceSupplier.isMultitenant());
        }
        return namespaceValidator;
    }

    private void writeSmallBulk(Entity[] entities) {
        actor.put(entities);
    }

    private static Logger log() {
        return LoggerSingleton.INSTANCE.logger;
    }

    private enum LoggerSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger logger = LoggerFactory.getLogger(DatastoreWrapper.class);
    }
}
