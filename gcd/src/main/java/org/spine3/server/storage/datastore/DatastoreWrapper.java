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
import com.google.cloud.datastore.Transaction;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newLinkedList;
import static java.lang.Math.min;

/**
 * Represents a wrapper above GAE {@link Datastore}.
 * <p>Provides API for Datastore to be used in storages.
 *
 * @author Dmytro Dashenkov
 */
class DatastoreWrapper {

    private static final String ACTIVE_TRANSACTION_CONDITION_MESSAGE = "Transaction should be active.";
    private static final String NOT_ACTIVE_TRANSACTION_CONDITION_MESSAGE = "Transaction should NOT be active.";

    private static final int MAX_KEYS_PER_READ_REQUEST = 1000;
    private static final int MAX_KEYS_PER_WRITE_REQUEST = 500;

    private static final Map<String, KeyFactory> keyFactories = new HashMap<>();

    private final Datastore datastore;
    private Transaction activeTransaction;
    private DatastoreReaderWriter actor;

    DatastoreWrapper(Datastore datastore) {
        this.datastore = datastore;
        this.actor = datastore;
    }

    /**
     * Wraps {@link Datastore} into an instance of {@code DatastoreWrapper} and returns the instance.
     *
     * @param datastore {@link Datastore} to wrap
     * @return new instance of {@code DatastoreWrapper}
     */
    static DatastoreWrapper wrap(Datastore datastore) {
        return new DatastoreWrapper(datastore);
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
        actor.put(entities);
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
     * @return A list of found entities in the order of keys (including {@code null} values for nonexistent keys)
     * @see DatastoreReader#fetch(Key...)
     */
    public List<Entity> read(Iterable<Key> keys) {
        final List<Key> keysList = Lists.newLinkedList(keys);
        final List<Entity> result;
        if (keysList.size() <= MAX_KEYS_PER_READ_REQUEST) {
            result = readSmallBulk(keysList);
        } else {
            result = readBigBulk(keysList);
        }
        return result;
    }

    /**
     * Queries the Datastore with the given arguments.
     * <p>
     * <p>As the Datastore may return a partial result set for {@link EntityQuery}, {@link KeyQuery}
     * and {@link ProjectionEntityQuery}, it is required to repeat a query with the adjusted cursor position.
     * <p>
     * <p>Therefore, an execution of this method may in fact result in several queries to the Datastore instance.
     *
     * @param query {@link Query} to execute upon the Datastore
     * @return results fo the query packed in a {@link List}
     * @see DatastoreReader#run(Query)
     */
    @SuppressWarnings({"unchecked", "IfStatementWithTooManyBranches", "ChainOfInstanceofChecks"})
    public List<Entity> read(Query query) {
        QueryResults queryResults = actor.run(query);
        final List<Entity> resultsAsList = newLinkedList();

        while (queryResults != null && queryResults.hasNext()) {
            Iterators.addAll(resultsAsList, queryResults);

            final Cursor cursorAfter = queryResults.getCursorAfter();
            final Query queryForMoreResults;

            /**
             * The generic {@link Query} cannot be transformed into the {@code Builder} instance due to different
             * nature of builders per {@code Query} subclass.
             *
             * <p>That's why the only way to repeat the same query with the cursor position adjusted is
             * to cast the {@code Query} instance to its subclass. Subclass instances may be transformed into
             * the {@code Builder}s, allowing to inject a new {@code startCursor} value.
             **/

            if (query instanceof EntityQuery) {
                final EntityQuery entityQuery = (EntityQuery) query;
                queryForMoreResults = entityQuery.toBuilder()
                        .setStartCursor(cursorAfter)
                        .build();
            } else if (query instanceof KeyQuery) {
                final KeyQuery keyQuery = (KeyQuery) query;
                queryForMoreResults = keyQuery.toBuilder()
                        .setStartCursor(cursorAfter)
                        .build();
            } else if (query instanceof ProjectionEntityQuery) {
                final ProjectionEntityQuery peQuery = (ProjectionEntityQuery) query;
                queryForMoreResults = peQuery.toBuilder()
                        .setStartCursor(cursorAfter)
                        .build();
            } else {
                queryForMoreResults = null;
            }

            queryResults = queryForMoreResults == null ? null : actor.run(queryForMoreResults);
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
        final Query query = Query.newEntityQueryBuilder()
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

        if (keysArray.length > MAX_KEYS_PER_WRITE_REQUEST) {
            int start = 0;
            int end = MAX_KEYS_PER_WRITE_REQUEST;
            while (true) {
                final int length = end - start;
                if (length <= 0) {
                    return;
                }
                final Key[] keysSubarray = new Key[length];
                System.arraycopy(keysArray, start, keysSubarray, 0, keysSubarray.length);
                delete(keysSubarray);

                start = end;
                end = min(MAX_KEYS_PER_WRITE_REQUEST, keysArray.length - end);
            }
        } else {
            delete(keysArray);
        }
    }

    /**
     * Starts a transaction.
     * <p>
     * <p>After this method is called, all {@code Entity} modifications performed through this instance of
     * {@code DatastoreWrapper} become transactional. This behaviour lasts until either {@link #commitTransaction()} or
     * {@link #rollbackTransaction()} is called.
     *
     * @throws IllegalStateException if a transaction is already started on this instance of {@code DatastoreWrapper}
     * @see #isTransactionActive()
     */
    @SuppressWarnings("WeakerAccess")
    // Part of API
    public void startTransaction() throws IllegalStateException {
        checkState(!isTransactionActive(), NOT_ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction = datastore.newTransaction();
        actor = activeTransaction;
    }

    /**
     * Commits a transaction.
     * <p>
     * <p>Upon the method call, all the modifications within the active transaction are applied.
     * <p>
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
     * <p>
     * <p>Upon the method call, all the modifications within the active transaction canceled permanently.
     * <p>
     * <p>After this method execution is over, all the further modifications made through the current instance of
     * {@code DatastoreWrapper} become non-transactional.
     *
     * @throws IllegalStateException if no transaction is active for the current instance of {@code DatastoreWrapper}
     * @see #isTransactionActive()
     */
    @SuppressWarnings("WeakerAccess")
    // Part of API
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
    @SuppressWarnings("WeakerAccess")
    // Part of API
    public boolean isTransactionActive() {
        return activeTransaction != null && activeTransaction.isActive();
    }

    /**
     * Retrieves an instance of {@link KeyFactory} unique for given Kind of data.
     * <p>
     * <p>Retrieved instances are the same across all instances of {@code DatastoreWrapper}.
     *
     * @param kind kind of {@link Entity} to generate keys for
     * @return an instance of {@link KeyFactory} for given kind
     */
    public KeyFactory getKeyFactory(String kind) {
        KeyFactory keyFactory = keyFactories.get(kind);
        if (keyFactory == null) {
            keyFactory = initKeyFactory(kind);
        }

        return keyFactory;
    }

    private KeyFactory initKeyFactory(String kind) {
        final KeyFactory keyFactory = datastore.newKeyFactory()
                .setKind(kind);
        keyFactories.put(kind, keyFactory);
        return keyFactory;
    }

    private List<Entity> readBigBulk(List<Key> keys) {
        final int pageCount = keys.size() / MAX_KEYS_PER_READ_REQUEST + 1;
        log().debug("Reading a big bulk of entities synchronously. The data is read as {} pages.", pageCount);

        final List<Entity> result = newLinkedList();
        int lowerBound = 0;
        int higherBound = MAX_KEYS_PER_READ_REQUEST;
        int keysLeft = keys.size();
        for (int i = 0; i < pageCount; i++) {
            final List<Key> keysPage = keys.subList(lowerBound, higherBound);

            final List<Entity> page = readSmallBulk(keysPage);
            result.addAll(page);

            keysLeft -= keysPage.size();
            lowerBound = higherBound;
            higherBound += min(keysLeft, MAX_KEYS_PER_READ_REQUEST);
        }

        return result;
    }

    private List<Entity> readSmallBulk(Iterable<Key> keys) {
        return Lists.newArrayList(datastore.get(keys));
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
