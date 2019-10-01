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

package io.spine.testing.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.flogger.FluentLogger;
import io.spine.annotation.Internal;
import io.spine.server.storage.datastore.DatastoreStorageFactory;
import io.spine.server.storage.datastore.DatastoreWrapper;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/**
 * Creates storages based on the local Google {@link Datastore}.
 */
public class TestDatastoreStorageFactory extends DatastoreStorageFactory {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static @MonotonicNonNull TestDatastoreStorageFactory instance = null;

    /**
     * Returns a default factory instance. A {@link Datastore} is created with
     * default {@link DatastoreOptions}:
     *
     * <p>Dataset name: {@code spine-dev}
     *
     * <p>Connects to a localhost Datastore emulator.
     */
    public static synchronized TestDatastoreStorageFactory defaultInstance() {
        try {
            if (instance == null) {
                instance = createInstance();
            }
            return instance;
        } catch (Throwable e) {
            logger.atSevere()
                  .withCause(e)
                  .log("Failed to initialize local datastore factory.");
            throw new IllegalStateException(e);
        }
    }

    private static TestDatastoreStorageFactory createInstance() {
        return new TestDatastoreStorageFactory(TestDatastores.local());
    }

    protected TestDatastoreStorageFactory(Datastore datastore) {
        super(DatastoreStorageFactory
                      .newBuilder()
                      .setDatastore(datastore)
                      .setTypeRegistry(DatastoreTypeRegistryFactory.defaultInstance())
        );
    }

    @Internal
    @Override
    protected DatastoreWrapper createDatastoreWrapper(boolean multitenant) {
        return TestDatastoreWrapper.wrap(datastore(), false);
    }

    /**
     * Performs operations on setting up the local datastore.
     *
     * <p>By default is a NO-OP, but can be overridden.
     */
    public void setUp() {
        // NO-OP. See doc.
    }

    /**
     * Clears all data in the local Datastore.
     *
     * <p>May be effectively the same as {@link #clear()}.
     *
     * <p><b>NOTE</b>: does not stop the server but just deletes all records.
     *
     * <p>Equivalent to dropping all tables in an SQL-base storage.
     */
    public void tearDown() {
        clear();
    }

    /**
     * Clears all data in the Datastore.
     *
     * @see #tearDown()
     */
    public void clear() {
        Iterable<DatastoreWrapper> wrappers = wrappers();
        for (DatastoreWrapper wrapper : wrappers) {
            TestDatastoreWrapper datastore = (TestDatastoreWrapper) wrapper;
            try {
                datastore.dropAllTables();
            } catch (Throwable e) {
                logger.atSevere()
                      .withCause(e)
                      .log("Unable to drop tables in Datastore `%s`.", datastore);
                throw new IllegalStateException(e);
            }
        }
    }
}
