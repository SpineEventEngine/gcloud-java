/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.annotations.VisibleForTesting;
import io.spine.io.Resource;
import io.spine.logging.WithLogging;
import io.spine.server.storage.datastore.ProjectId;

import java.io.IOException;

import static com.google.auth.oauth2.ServiceAccountCredentials.fromStream;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.io.Resource.file;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * A factory of test {@link Datastore} instances.
 */
public final class TestDatastores implements WithLogging {

    /**
     * The default port to which the local Datastore emulator is bound.
     *
     * <p>See<a href="https://cloud.google.com/sdk/gcloud/reference/beta/emulators/datastore/start">
     * {@code gcloud} docs</a>.
     */
    private static final int DEFAULT_EMULATOR_PORT = 8081;

    /**
     * The default project ID to use when running on a local Datastore emulator.
     */
    @VisibleForTesting
    static final ProjectId DEFAULT_LOCAL_PROJECT_ID = ProjectId.of("test-project");

    /**
     * Prevents instantiation of this utility class.
     */
    private TestDatastores() {
    }

    /**
     * Creates a {@link Datastore} connected to the Docker-based Datastore emulator.
     *
     * <p>The {@linkplain #DEFAULT_LOCAL_PROJECT_ID default project ID} will be used. For most
     * tests, it's okay to use this ID even if some other project ID was passed to the emulator via
     * the {@code --project} switch.
     *
     * <p>If, for some reason, you need to specify a custom project ID, please use
     * {@link #local(ProjectId) local(ProjectId)}.
     */
    public static Datastore local() {
        return local(DEFAULT_LOCAL_PROJECT_ID);
    }

    /**
     * Creates a {@link Datastore} connected to the Docker-based Datastore emulator
     * which runs with the specified project ID.
     */
    public static Datastore local(ProjectId projectId) {
        checkNotNull(projectId);
        var options = Emulator.at(projectId, DEFAULT_EMULATOR_PORT);
        var datastore = options.getService();
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
        return remote(file(serviceAccountPath, TestDatastores.class.getClassLoader()));
    }

    /**
     * Creates a {@link Datastore} connected to the remote Google Cloud Datastore described by the
     * given service account resource.
     */
    public static Datastore remote(Resource serviceAccount) {
        checkNotNull(serviceAccount);
        var credentials = credentialsFrom(serviceAccount);
        var options = DatastoreOptions.newBuilder()
                .setCredentials(credentials)
                .build();
        var datastore = options.getService();
        return datastore;
    }

    private static Credentials credentialsFrom(Resource serviceAccount) {
        try {
            var is = serviceAccount.open();
            var credentials = fromStream(is);
            return credentials;
        } catch (IOException e) {
            throw newIllegalStateException(
                    e, "Unable to parse Service Account credentials from `%s` resource.",
                    serviceAccount
            );
        }
    }

    /**
     * Returns the default project ID that is used when running on a local Datastore emulator.
     */
    public static ProjectId defaultLocalProjectId() {
        return DEFAULT_LOCAL_PROJECT_ID;
    }
}
