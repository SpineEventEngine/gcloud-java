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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Custom extension of the {@link DatastoreWrapper} aimed on testing purposes.
 *
 * @author Dmytro Dashenkov
 * @see TestDatastoreStorageFactory
 */
/*package*/ class TestDatastoreWrapper extends DatastoreWrapper {

    // Default time to wait before each read operation to ensure the data is consistent.
    // NOTE: enabled only if {@link #shouldWaitForConsistency} is {@code true}.
    private static final int CONSISTENCY_AWAIT_TIME_MS = 100;

    private static final int CONSISTENCY_AWAIT_ITERATIONS = 5;

    private static final Collection<String> kindsCache = new LinkedList<>();

    private final boolean waitForConsistency;

    private TestDatastoreWrapper(Datastore datastore, boolean waitForConsistency) {
        super(datastore);
        this.waitForConsistency = waitForConsistency;
    }

    /*package*/ static TestDatastoreWrapper wrap(Datastore datastore, boolean waitForConsistency) {
        return new TestDatastoreWrapper(datastore, waitForConsistency);
    }

    @Override
    public KeyFactory getKeyFactory(String kind) {
        kindsCache.add(kind);
        return super.getKeyFactory(kind);
    }

    @Override
    Entity read(Key key) {
        waitForConsistency();
        return super.read(key);
    }

    @Override
    List<Entity> read(Iterable<Key> keys) {
        waitForConsistency();
        return super.read(keys);
    }

    @Override
    List<Entity> read(Query query) {
        waitForConsistency();
        return super.read(query);
    }

    private void waitForConsistency() {
        if (!waitForConsistency) {
            log().info("Wait for consistency is not required.");
            return;
        }
        log().info("Waiting for data consistency to establish.");

        for(int awaitCycle = 0; awaitCycle < CONSISTENCY_AWAIT_ITERATIONS; awaitCycle++) {
            try {
                Thread.sleep(CONSISTENCY_AWAIT_TIME_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Deletes all records from the datastore.
     */
    /*package*/ void dropAllTables() {
        log().info("Dropping all tables");
        for (String kind : kindsCache) {
            dropTable(kind);
        }

        kindsCache.clear();
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(TestDatastoreWrapper.class);
    }
}
