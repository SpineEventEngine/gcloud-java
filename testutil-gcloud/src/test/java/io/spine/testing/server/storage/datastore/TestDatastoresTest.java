/*
 * Copyright 2021, TeamDev. All rights reserved.
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

import io.spine.io.Resource;
import io.spine.server.storage.datastore.ProjectId;
import io.spine.testing.UtilityClassTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.testing.server.storage.datastore.TestDatastores.DEFAULT_EMULATOR_PORT;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`TestDatastores` utility should")
class TestDatastoresTest extends UtilityClassTest<TestDatastores> {

    TestDatastoresTest() {
        super(TestDatastores.class);
    }

    @Nested
    @DisplayName("create a `Datastore` connected to the local Datastore emulator running at")
    class CreateLocalDatastore {

        private static final String ADDRESS_FORMAT = "localhost:%d";

        @Test
        @DisplayName("the default emulator address")
        void atDefaultAddress() {
            var datastore = TestDatastores.local();
            var host = datastore.getOptions()
                                .getHost();
            var expectedHost = format(ADDRESS_FORMAT, DEFAULT_EMULATOR_PORT);
            assertThat(host).isEqualTo(expectedHost);
        }

        @Test
        @DisplayName("a custom port")
        void atCustomPort() {
            var port = 8080;
            var datastore = TestDatastores.local(port);
            var host = datastore.getOptions()
                                .getHost();
            var expectedHost = format(ADDRESS_FORMAT, port);
            assertThat(host).isEqualTo(expectedHost);
        }

        @Test
        @DisplayName("a custom port with a custom project ID")
        void atCustomPortWithCustomId() {
            var port = 8080;
            var id = "the-test-project";
            var projectId = ProjectId.of(id);
            var datastore = TestDatastores.local(projectId, port);

            var options = datastore.getOptions();
            var host = options.getHost();
            var expectedHost = format(ADDRESS_FORMAT, port);
            assertThat(host).isEqualTo(expectedHost);

            var actualProjectId = options.getProjectId();
            assertThat(actualProjectId).isEqualTo(id);
        }
    }

    @Nested
    @DisplayName("create a `Datastore` connected to the remote Datastore described by")
    class CreateRemoteDatastore {

        private static final String SPINE_DEV_JSON = "spine-dev.json";
        private static final String PROJECT_ID = "spine-dev";

        @Test
        @DisplayName("the service account resource at path")
        void byResourceAtPath() {
            var datastore = TestDatastores.remote(SPINE_DEV_JSON);
            var projectId = datastore.getOptions()
                                     .getProjectId();
            assertThat(projectId).isEqualTo(PROJECT_ID);
        }

        @Test
        @DisplayName("the service account resource")
        void byResource() {
            var serviceAccount = localResource(SPINE_DEV_JSON);
            var datastore = TestDatastores.remote(serviceAccount);
            var projectId = datastore.getOptions()
                                     .getProjectId();
            assertThat(projectId).isEqualTo(PROJECT_ID);
        }
    }

    @Test
    @DisplayName("throw an `ISE` when can't properly parse account credentials from resource")
    void throwOnInvalidResource() {
        var resource = localResource("random.json");
        assertThrows(IllegalStateException.class, () -> TestDatastores.remote(resource));
    }

    private static Resource localResource(String path) {
        return Resource.file(path, TestDatastoresTest.class.getClassLoader());
    }
}
