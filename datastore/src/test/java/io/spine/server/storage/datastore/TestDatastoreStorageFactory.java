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
import io.spine.logging.Logging;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;

import static io.spine.server.storage.datastore.TestEnvironment.runsOnCi;

/**
 * Creates storages based on the local Google {@link Datastore}.
 */
public class TestDatastoreStorageFactory extends DatastoreStorageFactory {

    static final String DEFAULT_DATASET_NAME = TestDatastores.projectId().getValue();

    private static @MonotonicNonNull TestDatastoreStorageFactory instance = null;

    /**
     * Returns a default factory instance. A {@link Datastore} is created with
     * default {@link DatastoreOptions}:
     *
     * <p>Dataset name: {@code spine-dev}
     *
     * <p>Connects to a localhost Datastore emulator or to a remote Datastore if run on CI.
     */
    static synchronized TestDatastoreStorageFactory getDefaultInstance() {
        if (instance == null) {
            boolean onCi = runsOnCi();
            instance = onCi
                        ? createCiInstance()
                        : createLocalInstance();
        }
        return instance;
    }

    private static TestDatastoreStorageFactory createLocalInstance() {
        log().info("Running on local machine. Connecting to a local Datastore emulator.");
        return new TestDatastoreStorageFactory(TestDatastores.local());
    }

    private static TestDatastoreStorageFactory createCiInstance() {
        log().info("Running on CI. Connecting to remote Google Cloud Datastore.");
        return new TestDatastoreStorageFactory(TestDatastores.remote());
    }

    protected TestDatastoreStorageFactory(Datastore datastore) {
        super(DatastoreStorageFactory
                      .newBuilder()
                      .setDatastore(datastore)
                      .setMultitenant(false)
                      .setTypeRegistry(DatastoreTypeRegistryFactory.defaultInstance())
                      .setNamespaceSupplier(NamespaceSupplier.singleTenant())
        );
    }

    @Override
    protected DatastoreWrapper createDatastoreWrapper(Builder builder) {
        return TestDatastoreWrapper.wrap(builder.getDatastore(), runsOnCi());
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

    private static Logger log() {
        return Logging.get(TestDatastoreStorageFactory.class);
    }
}
