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

package io.spine.server.storage.datastore.given;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import io.spine.logging.Logging;
import io.spine.server.storage.datastore.ProjectId;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.google.auth.oauth2.ServiceAccountCredentials.fromStream;

/**
 * Provides test {@link Datastore} instances.
 */
public class TestDatastores {

    private static final ProjectId TEST_PROJECT_ID = ProjectId.of("spine-dev");

    /** Prevent this test utility class from being instantiated. */
    private TestDatastores() {
    }

    /**
     * Obtains Datastore instance used for testing on a developer's workstation.
     */
    public static Datastore local() {
        return Local.INSTANCE.getService();
    }

    /**
     * Obtains Datastore instance used for testing in a CI environment.
     */
    public static Datastore remote() {
        return Ci.INSTANCE.getService();
    }

    /**
     * Obtains ProjectId of the test environment.
     */
    public static ProjectId projectId() {
        return TEST_PROJECT_ID;
    }

    /**
     * Abstract base for options factories.
     */
    private abstract static class Options {

        private final DatastoreOptions.Builder builder;

        private Options() {
            builder = DatastoreOptions.newBuilder()
                                      .setProjectId(projectId().getValue());
        }

        final DatastoreOptions.Builder builder() {
            return builder;
        }

        final DatastoreOptions create() {
            return builder.build();
        }
    }

    /**
     * Local Datastore options used on developers' machines.
     */
    private static final class Local extends Options {

        private static final String DEFAULT_HOST = "localhost:8080";
        private static final DatastoreOptions INSTANCE = new Local().create();

        private Local() {
            super();
            builder().setHost(DEFAULT_HOST);
            builder().setCredentials(NoCredentials.getInstance());
        }
    }

    /**
     * Options used to run tests in CI environment, which uses connection to
     * a real Datastore instance in the testing Google Cloud environment.
     */
    @SuppressWarnings("NewClassNamingConvention")
    private static final class Ci extends Options implements Logging {

        private static final String CREDENTIALS_FILE_PATH = "/spine-dev.json";
        private static final DatastoreOptions INSTANCE = new Ci().create();

        private Ci() {
            try {
                InputStream is = TestDatastores.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
                BufferedInputStream bufferedStream = new BufferedInputStream(is);
                ServiceAccountCredentials credentials = fromStream(bufferedStream);
                builder().setCredentials(credentials);
            } catch (@SuppressWarnings("OverlyBroadCatchBlock") IOException e) {
                _warn().log("Cannot find the credentials file `%s`.", CREDENTIALS_FILE_PATH);
            }
        }
    }
}
