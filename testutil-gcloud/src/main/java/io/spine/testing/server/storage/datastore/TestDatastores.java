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

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.annotations.VisibleForTesting;
import io.spine.io.Resource;
import io.spine.logging.Logging;
import io.spine.server.storage.datastore.ProjectId;

import java.io.IOException;
import java.io.InputStream;

import static com.google.auth.oauth2.ServiceAccountCredentials.fromStream;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.io.Resource.file;
import static io.spine.util.Exceptions.newIllegalArgumentException;
import static java.lang.String.format;

/**
 * A factory of test {@link Datastore} instances.
 */
public final class TestDatastores implements Logging {

    /**
     * The default port to which the local Datastore emulator is bound.
     *
     * <p>See<a href="https://cloud.google.com/sdk/gcloud/reference/beta/emulators/datastore/start">
     * {@code gcloud} docs</a>.
     */
    @VisibleForTesting
    static final int DEFAULT_EMULATOR_PORT = 8081;
    private static final String LOCALHOST = "localhost";

    /**
     * The project ID which is used when running on local Datastore emulator.
     */
    private static final ProjectId LOCAL_PROJECT_ID = ProjectId.of("test-project");

    /** Prevents instantiation of this utility class. */
    private TestDatastores() {
    }

    /**
     * Creates a {@link Datastore} connected to the local Datastore emulator at
     * {@link #DEFAULT_EMULATOR_PORT}.
     */
    public static Datastore local() {
        return local(DEFAULT_EMULATOR_PORT);
    }

    /**
     * Creates a {@link Datastore} connected to the local Datastore emulator at the specified port.
     */
    public static Datastore local(int port) {
        String address = format("%s:%d", LOCALHOST, port);

        DatastoreOptions options = DatastoreOptions
                .newBuilder()
                .setProjectId(LOCAL_PROJECT_ID.value())
                .setHost(address)
                .setCredentials(NoCredentials.getInstance())
                .build();
        Datastore datastore = options.getService();
        return datastore;
    }

    /**
     * Creates a {@link Datastore} connected to the remote Google Cloud Datastore described by the
     * given service account resource.
     *
     * <p>The {@code serviceAccountPath} is a path to the resource file, specified relative to the
     * classpath.
     */
    public static Datastore remote(String serviceAccountPath) {
        checkNotNull(serviceAccountPath);
        return remote(file(serviceAccountPath));
    }

    /**
     * Creates a {@link Datastore} connected to the remote Google Cloud Datastore described by the
     * given service account resource.
     */
    public static Datastore remote(Resource serviceAccount) {
        checkNotNull(serviceAccount);
        try {
            Credentials credentials = credentialsFrom(serviceAccount);
            DatastoreOptions options = DatastoreOptions
                    .newBuilder()
                    .setCredentials(credentials)
                    .build();
            Datastore datastore = options.getService();
            return datastore;
        } catch (IOException e) {
            throw newIllegalArgumentException(
                    e, "Cannot find the credentials file `%s`.", serviceAccount);
        }
    }

    private static Credentials credentialsFrom(Resource serviceAccount) throws IOException {
        InputStream is = serviceAccount.open();
        ServiceAccountCredentials credentials = fromStream(is);
        return credentials;
    }

    public static ProjectId localProjectId() {
        return LOCAL_PROJECT_ID;
    }
}
