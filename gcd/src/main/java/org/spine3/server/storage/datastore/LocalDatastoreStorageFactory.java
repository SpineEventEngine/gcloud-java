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
import com.google.cloud.datastore.DatastoreOptions;
import org.spine3.server.storage.datastore.newapi.TestDatastoreWrapper;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;

/**
 * Creates storages based on the local Google {@link Datastore}.
 */
@SuppressWarnings("CallToSystemGetenv")
public class LocalDatastoreStorageFactory extends DatastoreStorageFactory {

    private static final String DEFAULT_DATASET_NAME = "spine-dev";
    private static final String DEFAULT_HOST = "localhost:8080";

    private static final DatastoreOptions DEFAULT_LOCAL_OPTIONS = DatastoreOptions.builder()
            .projectId(DEFAULT_DATASET_NAME)
            .host(DEFAULT_HOST)
            .build();

    private static final String OPTION_TESTING_MODE = "--testing";

    private static final String VAR_NAME_GCD_HOME = "GCD_HOME";

    private static final String GCD_HOME_PATH = retrieveGcdHome();

    private static final String ENVIRONMENT_NOT_CONFIGURED_MESSAGE = VAR_NAME_GCD_HOME + " environment variable is not configured. " +
            "See https://github.com/SpineEventEngine/core-java/wiki/Configuring-Local-Datastore-Environment";

    /**
     * Returns a default factory instance. A {@link Datastore} is created with default {@link DatastoreOptions}:
     *
     * <p>Dataset name: {@code spine-local-dataset}
     *
     * <p>Host: {@code http://localhost:8080}
     */
    public static LocalDatastoreStorageFactory getDefaultInstance() {
        return DefaultInstanceSingleton.INSTANCE.value;
    }

    /**
     * Creates a new factory instance.
     *
     * @param options {@link DatastoreOptions} used to create a {@link Datastore}
     */
    public static LocalDatastoreStorageFactory newInstance(DatastoreOptions options) {
        final Datastore datastore = options.service();
        return new LocalDatastoreStorageFactory(datastore);
    }

    private LocalDatastoreStorageFactory(Datastore datastore) {
        super(datastore, false);
    }

    @SuppressWarnings("RefusedBequest")
    @Override
    protected void initDatastoreWrapper(Datastore datastore) {
        checkState(this.datastore == null, "Datastore is already inited.");
        this.datastore = TestDatastoreWrapper.wrap(datastore);
    }

    /**
     * Intended to start the local Datastore server in testing mode.
     * <p>
     * NOTE: does nothing for now because of several issues:
     * This <a href="https://github.com/GoogleCloudPlatform/google-cloud-datastore/commit/a077c5b4d6fa2826fd6c376b692686894b719fd9">commit</a>
     * seems to fix the first issue, but there is no release with this fix available yet.
     * Also fails to start on Windows:
     * <a href="https://code.google.com/p/google-cloud-platform/issues/detail?id=10&thanks=10&ts=1443682670">issue</a>.
     * <p>
     * Until these issues are not fixed, it is required to start the local Datastore Server manually.
     * See <a href="https://github.com/SpineEventEngine/core-java/wiki/Configuring-Local-Datastore-Environment">docs</a> for details.<br>
     *
     * @throws RuntimeException if {@link Datastore#start(String, String, String...)}
     *                          throws LocalDevelopmentDatastoreException.
     * @see <a href="https://cloud.google.com/DATASTORE/docs/tools/devserver#local_development_server_command-line_arguments">
     * Documentation</a> ("testing" option)
     */
    public void setUp() {
        if (false) // TODO:2015-11-12:alexander.litus: Remove the condition when issues specified above are fixed
        try {
            //localDatastore.start(GCD_HOME_PATH, DEFAULT_DATASET_NAME, OPTION_TESTING_MODE); // TODO:11-10-16:dmytro.dashenkov: Resolve.
        } catch (RuntimeException e) { // DatastoreException
            propagate(e);
        }
    }

    /**
     * Clears all data in the local Datastore.
     * <p>
     * NOTE: does not stop the server because of several issues. See {@link #setUp()} method doc for details.
     *
     * @throws RuntimeException if {@link Datastore#stop()} throws LocalDevelopmentDatastoreException.
     */
    public void tearDown() {
        clear();
        if (false) // TODO:2015-11-12:alexander.litus: remove the condition when issues specified in setUp method javadoc are fixed
        try {
            //localDatastore.stop(); // TODO:11-10-16:dmytro.dashenkov: Resolve.
        } catch (RuntimeException e) { // DatastoreException
            propagate(e);
        }
    }

    /**
     * Clears all data in the local Datastore.
     * // TODO:19-10-16:dmytro.dashenkov: Fix javadocs.
     * @throws RuntimeException if {@link Datastore#clear()} throws LocalDevelopmentDatastoreException.
     */
    /* package */ void clear() {
        ((TestDatastoreWrapper) datastore).dropAllTables();
    }

    private static String retrieveGcdHome() {
        final String gcdHome = System.getenv(VAR_NAME_GCD_HOME);
        checkState(gcdHome != null, ENVIRONMENT_NOT_CONFIGURED_MESSAGE);
        return gcdHome;
    }

    private enum DefaultInstanceSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final LocalDatastoreStorageFactory value = new LocalDatastoreStorageFactory(DefaultDatastoreSingleton.INSTANCE.value);
    }

    private enum DefaultDatastoreSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Datastore value = DEFAULT_LOCAL_OPTIONS.service();
    }
}
