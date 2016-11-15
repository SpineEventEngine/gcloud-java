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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkState;

/**
 * Creates storages based on the local Google {@link Datastore}.
 */
@SuppressWarnings("CallToSystemGetenv")
/* package */ class TestDatastoreStorageFactory extends DatastoreStorageFactory {

    private static final String DEFAULT_DATASET_NAME = "spine-dev";
    private static final String DEFAULT_HOST = "localhost:8080";
    private static final String CREDENTIALS_FILE_PATH = "/spine-dev-62685282c0b9.json";

    private static final DatastoreOptions DEFAULT_LOCAL_OPTIONS = DatastoreOptions.newBuilder()
                                                                                  .setProjectId(DEFAULT_DATASET_NAME)
                                                                                  .setHost(DEFAULT_HOST)
                                                                                  .build();

    // Set in the static context upon initialization,
    // used in class method context to avoid ambiguous calls to {@code System.getenv()}.
    @SuppressWarnings("StaticNonFinalField")
    private static boolean runsOnCi = false;

    private static final DatastoreOptions TESTING_OPTIONS = generateTestOptions();

    private static DatastoreOptions generateTestOptions() {
        try {
            final InputStream is = TestDatastoreStorageFactory.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
            final BufferedInputStream bufferedStream = new BufferedInputStream(is);

            final ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(bufferedStream);
            return DatastoreOptions.newBuilder()
                                   .setProjectId(DEFAULT_DATASET_NAME).setCredentials(credentials)
                                   .build();
        } catch (@SuppressWarnings("OverlyBroadCatchBlock") IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a default factory instance. A {@link Datastore} is created with default {@link DatastoreOptions}:
     *
     * <p>Dataset name: {@code spine-dev}
     *
     * <p>Connects to a localhost Datastore emulator or to a remote Datastore if run on CI.
     */
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    /* package */ static TestDatastoreStorageFactory getDefaultInstance() {
        final boolean onCi = "true".equals(System.getenv("CI"));
        final String message = onCi
                               ? "Running on CI. Connecting to remote Google Cloud Datastore"
                               : "Running on local machine. Connecting to a local Datastore emulator";
        log().info(message);
        runsOnCi = onCi;
        return onCi
               ? TestingInstanceSingleton.INSTANCE.value
               : LocalInstanceSingleton.INSTANCE.value;
    }

    private TestDatastoreStorageFactory(Datastore datastore) {
        super(datastore, false);
    }

    @SuppressWarnings("RefusedBequest")
    @Override
    protected void initDatastoreWrapper(Datastore datastore) {
        checkState(this.getDatastore() == null, "Datastore is already initialized.");
        this.setDatastore(TestDatastoreWrapper.wrap(datastore, runsOnCi));
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
    /* package */ void clear() {
        ((TestDatastoreWrapper) getDatastore()).dropAllTables();
    }

    private enum LocalInstanceSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final TestDatastoreStorageFactory value =
                new TestDatastoreStorageFactory(DefaultDatastoreSingleton.INSTANCE.value);
    }

    private enum TestingInstanceSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final TestDatastoreStorageFactory value =
                new TestDatastoreStorageFactory(TestingDatastoreSingleton.INSTANCE.value);
    }

    private enum DefaultDatastoreSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Datastore value = DEFAULT_LOCAL_OPTIONS.getService();
    }

    private enum TestingDatastoreSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Datastore value = TESTING_OPTIONS.getService();
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
