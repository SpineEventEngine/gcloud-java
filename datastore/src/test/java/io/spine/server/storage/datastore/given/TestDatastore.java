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

package io.spine.server.storage.datastore.given;

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import io.spine.server.storage.datastore.ProjectId;

/**
 * Provides test {@link Datastore} instance.
 */
public class TestDatastore {

    private static final ProjectId TEST_PROJECT_ID = ProjectId.of("spine-dev");
    private static final String DEFAULT_HOST = "localhost:8080";
    private static final DatastoreOptions LOCAL_DATASTORE = localDatastoreOptions();

    /** Prevents this test utility class from being instantiated. */
    private TestDatastore() {
    }

    /**
     * Obtains Datastore instance used for testing on a developer's workstation.
     */
    public static Datastore instance() {
        return LOCAL_DATASTORE.getService();
    }

    /**
     * Obtains ProjectId of the test environment.
     */
    public static ProjectId projectId() {
        return TEST_PROJECT_ID;
    }

    private static DatastoreOptions localDatastoreOptions() {
        DatastoreOptions result = DatastoreOptions
                .newBuilder()
                .setProjectId(projectId().getValue())
                .setHost(DEFAULT_HOST)
                .setCredentials(NoCredentials.getInstance())
                .build();
        return result;
    }
}
