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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import io.spine.io.Resource;
import io.spine.server.storage.datastore.ProjectId;
import io.spine.testing.UtilityClassTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.testing.server.storage.datastore.TestDatastores.DEFAULT_EMULATOR_PORT;
import static java.lang.String.format;

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
            Datastore datastore = TestDatastores.local();
            String host = datastore.getOptions()
                                   .getHost();
            String expectedHost = format(ADDRESS_FORMAT, DEFAULT_EMULATOR_PORT);
            assertThat(host)
                    .isEqualTo(expectedHost);
        }

        @Test
        @DisplayName("a custom port")
        void atCustomPort() {
            int port = 8080;
            Datastore datastore = TestDatastores.local(port);
            String host = datastore.getOptions()
                                   .getHost();
            String expectedHost = format(ADDRESS_FORMAT, port);
            assertThat(host)
                    .isEqualTo(expectedHost);
        }

        @Test
        @DisplayName("a custom port with a custom project ID")
        void atCustomPortWithCustomId() {
            int port = 8080;
            String id = "the-test-project";
            ProjectId projectId = ProjectId.of(id);
            Datastore datastore = TestDatastores.local(projectId, port);

            DatastoreOptions options = datastore.getOptions();
            String host = options.getHost();
            String expectedHost = format(ADDRESS_FORMAT, port);
            assertThat(host)
                    .isEqualTo(expectedHost);

            String actualProjectId = options.getProjectId();
            assertThat(actualProjectId)
                    .isEqualTo(id);
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
            Datastore datastore = TestDatastores.remote(SPINE_DEV_JSON);
            String projectId = datastore.getOptions()
                                        .getProjectId();
            assertThat(projectId)
                    .isEqualTo(PROJECT_ID);
        }

        @Test
        @DisplayName("the service account resource")
        void byResource() {
            Resource serviceAccount = Resource.file(SPINE_DEV_JSON);
            Datastore datastore = TestDatastores.remote(serviceAccount);
            String projectId = datastore.getOptions()
                                        .getProjectId();
            assertThat(projectId)
                    .isEqualTo(PROJECT_ID);
        }
    }
}
