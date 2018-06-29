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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import static io.spine.server.storage.datastore.TestDatastoreStorageFactory.DEFAULT_DATASET_NAME;

/**
 * The factory for the test {@link Datastore} instances.
 *
 * @author Dmytro Dashenkov
 */
public class TestDatastoreFactory {

    private static final String DEFAULT_HOST = "localhost:8080";
    private static final String CREDENTIALS_FILE_PATH = "/spine-dev-62685282c0b9.json";
    private static final DatastoreOptions DEFAULT_LOCAL_OPTIONS =
            DatastoreOptions.newBuilder()
                            .setProjectId(DEFAULT_DATASET_NAME)
                            .setHost(DEFAULT_HOST)
                            .build();

    private static final DatastoreOptions TESTING_OPTIONS = generateTestOptions();

    private static DatastoreOptions generateTestOptions() {
        try {
            InputStream is = TestDatastoreFactory.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
            BufferedInputStream bufferedStream = new BufferedInputStream(is);

            ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(bufferedStream);
            return DatastoreOptions.newBuilder()
                                   .setProjectId(DEFAULT_DATASET_NAME)
                                   .setCredentials(credentials)
                                   .build();
        } catch (@SuppressWarnings("OverlyBroadCatchBlock") IOException e) {
            log().warn("Cannot find the configuration file {}", CREDENTIALS_FILE_PATH);
            DatastoreOptions defaultOptions = DatastoreOptions.newBuilder()
                                                                    .setProjectId(DEFAULT_DATASET_NAME)
                                                                    .build();
            return defaultOptions;
        }
    }

    public static Datastore getLocalDatastore() {
        return DefaultDatastoreSingleton.INSTANCE.value;
    }

    public static Datastore getTestRemoteDatastore() {
        return TestingDatastoreSingleton.INSTANCE.value;
    }

    private TestDatastoreFactory() {
        // Prevent this test utility class from being instantiated.
    }

    enum DefaultDatastoreSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        final Datastore value = DEFAULT_LOCAL_OPTIONS.getService();
    }

    enum TestingDatastoreSingleton {
        INSTANCE;
        @SuppressWarnings({"NonSerializableFieldInSerializableClass", "PackageVisibleField"})
        final Datastore value = TESTING_OPTIONS.getService();
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(TestDatastoreFactory.class);
    }
}
