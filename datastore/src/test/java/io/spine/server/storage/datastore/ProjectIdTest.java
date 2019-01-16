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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
class ProjectIdTest {

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void testNulls() {
        new NullPointerTester()
                .setDefault(Datastore.class, mockDatastore())
                .testStaticMethods(ProjectId.class, NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    @DisplayName("support equality")
    void testEquals() {
        ProjectId a1 = ProjectId.of("a");
        ProjectId a2 = ProjectId.of(mockDatastore("a"));

        ProjectId b1 = ProjectId.of("b");
        ProjectId b2 = ProjectId.of(mockDatastore("b"));

        new EqualsTester()
                .addEqualityGroup(a1, a2)
                .addEqualityGroup(b1, b2)
                .testEquals();
    }

    @Test
    @DisplayName("support toString()")
    void testToString() {
        String value = "my-fancy-project-id";
        ProjectId projectId = ProjectId.of(value);
        String stringRepr = projectId.toString();
        assertThat(stringRepr, containsString(value));
    }

    private static Datastore mockDatastore() {
        return mockDatastore("some-project-id-ProjectIdTest");
    }

    private static Datastore mockDatastore(String value) {
        Datastore datastore = mock(Datastore.class);
        DatastoreOptions options = mock(DatastoreOptions.class);
        when(datastore.getOptions()).thenReturn(options);
        when(options.getProjectId()).thenReturn(value);
        return datastore;
    }
}
