/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import com.google.cloud.datastore.DatastoreOptions;
import io.spine.server.storage.datastore.given.Given;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.spine.server.datastore.TestEnvironment.runsOnCi;

/**
 * Creates storages based on the local Google {@link Datastore}.
 */
public class TestDatastoreStorageFactory extends DatastoreStorageFactory {

    static final String DEFAULT_DATASET_NAME = Given.testProjectIdValue();

    /**
     * Returns a default factory instance. A {@link Datastore} is created with
     * default {@link DatastoreOptions}:
     *
     * <p>Dataset name: {@code spine-dev}
     *
     * <p>Connects to a localhost Datastore emulator or to a remote Datastore if run on CI.
     */
    static TestDatastoreStorageFactory getDefaultInstance() {
        boolean onCi = runsOnCi();
        String message = onCi
                         ? "Running on CI. Connecting to remote Google Cloud Datastore"
                         : "Running on local machine. Connecting to a local Datastore emulator";
        log().info(message);
        return onCi
               ? TestingInstanceSingleton.INSTANCE.value
               : LocalInstanceSingleton.INSTANCE.value;
    }

    protected TestDatastoreStorageFactory(Datastore datastore) {
        super(DatastoreStorageFactory
                      .newBuilder()
                      .setDatastore(datastore)
                      .setMultitenant(false)
                      .setTypeRegistry(DatastoreTypeRegistryFactory.defaultInstance())
                      .setNamespaceSupplier(NamespaceSupplier.singleTenant())
                      .setNamespaceToTenantIdConverter(null)
        );
    }

    @Override
    protected DatastoreWrapper createDatastoreWrapper(Datastore datastore) {
        return TestDatastoreWrapper.wrap(datastore, runsOnCi());
    }

    /**
     * Performs operations on setting up local datastore.
     * <p>General usage is testing.
     * <p>By default is a NoOp, but can be overridden.
     */
    @SuppressWarnings("EmptyMethod")
    public void setUp() {
    }

    /**
     * Clears all data in the local Datastore.
     * <p>May be effectively the same as {@link #clear()}.
     *
     * <p>NOTE: does not stop the server but just deletes all records.
     * <p>Equivalent to dropping all tables in an SQL-base storage.
     */
    public void tearDown() {
        clear();
    }

    /**
     * Clears all data in the local Datastore.
     *
     * @see #tearDown()
     */
    void clear() {
        ((TestDatastoreWrapper) getDatastore()).dropAllTables();
    }

    private enum LocalInstanceSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final TestDatastoreStorageFactory value =
                new TestDatastoreStorageFactory(TestDatastoreFactory.getLocalDatastore());
    }

    private enum TestingInstanceSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final TestDatastoreStorageFactory value =
                new TestDatastoreStorageFactory(TestDatastoreFactory.getTestRemoteDatastore());
    }



    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(TestDatastoreStorageFactory.class);
    }
}
