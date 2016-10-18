/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.datastore.newapi;

import com.google.cloud.datastore.*;
import com.google.common.collect.Lists;
import org.spine3.server.storage.datastore.DatastoreStorageFactory;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author Dmytro Dashenkov
 */
public class DatastoreWrapper {

    private static final String ACTIVE_TRANSACTION_CONDITION_MESSAGE = "Transaction should be active.";
    private static final String NOT_ACTIVE_TRANSACTION_CONDITION_MESSAGE = "Transaction should NOT be active.";
    private final Datastore datastore;
    private Transaction activeTransaction;
    private DatastoreReaderWriter actor;
    private KeyFactory keyFactory;

    private DatastoreWrapper(Datastore datastore) {
        this.datastore = datastore;
        this.actor = datastore;
        this.keyFactory = datastore.newKeyFactory();
    }

    public static DatastoreWrapper newInstance(Datastore datastore, DatastoreStorageFactory.Options options) {
        return new DatastoreWrapper(datastore);
    }


    public void create(Entity entity) {
        actor.put(entity);
    }

    public void update(Entity entity) {
        actor.update(entity);
    }

    public void createOrUpdate(Entity entity) {
        try {
            create(entity);
        } catch (DatastoreException ignored) {
            update(entity);
        }
    }

    public Entity read(Key key) {
        return datastore.get(key);
    }

    // TODO:18-10-16:dmytro.dashenkov: Check datastore#fetch usage.
    public List<Entity> read(Iterable<Key> keys) {
        return Lists.newArrayList(datastore.get(keys));
    }

    @SuppressWarnings("unchecked")
    public List<Entity> read(Query query) {
        final QueryResults results = actor.run(query);
        return Lists.newArrayList(results);
    }

    public void delete(Key... keys) {
        actor.delete(keys);
    }

    public void startTransaction() {
        checkState(!isTransactionActive(), NOT_ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction = datastore.newTransaction();
        actor = activeTransaction;
    }

    public void commitTransaction() {
        checkState(isTransactionActive(), ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction.commit();
        actor = datastore;
    }

    public void rollbackTransaction() {
        checkState(isTransactionActive(), ACTIVE_TRANSACTION_CONDITION_MESSAGE);
        activeTransaction.rollback();
        actor = datastore;
    }

    public KeyFactory getKeyFactory() {
        return keyFactory;
    }

    private boolean isTransactionActive() {
        return activeTransaction != null && activeTransaction.active();
    }
}
