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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import io.spine.logging.Logging;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.google.auth.oauth2.ServiceAccountCredentials.fromStream;
import static io.spine.server.storage.datastore.TestDatastoreStorageFactory.DEFAULT_DATASET_NAME;

/**
 * Provides test {@link Datastore} instances.
 */
public class TestDatastores {

    private static final String DEFAULT_HOST = "localhost:8080";
    private static final String CREDENTIALS_FILE_PATH = "/spine-dev-62685282c0b9.json";

    private static final DatastoreOptions DEFAULT_LOCAL_OPTIONS =
            DatastoreOptions.newBuilder()
                            .setProjectId(DEFAULT_DATASET_NAME)
                            .setHost(DEFAULT_HOST)
                            .build();

    private static final DatastoreOptions TESTING_OPTIONS = generateTestOptions();

    private static DatastoreOptions generateTestOptions() {
        DatastoreOptions.Builder result = DatastoreOptions
                .newBuilder()
                .setProjectId(DEFAULT_DATASET_NAME);
        try {
            InputStream is = TestDatastores.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
            BufferedInputStream bufferedStream = new BufferedInputStream(is);
            ServiceAccountCredentials credentials = fromStream(bufferedStream);
            result.setCredentials(credentials);
        } catch (@SuppressWarnings("OverlyBroadCatchBlock") IOException e) {
            log().warn("Cannot find the credentials file {}.", CREDENTIALS_FILE_PATH);
        }

        return result.build();
    }

    public static Datastore local() {
        return DEFAULT_LOCAL_OPTIONS.getService();
    }

    public static Datastore remote() {
        return TESTING_OPTIONS.getService();
    }

    /** Prevent this test utility class from being instantiated. */
    private TestDatastores() {
    }

    private static Logger log() {
        return Logging.get(TestDatastores.class);
    }
}
