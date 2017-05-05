/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class ProjectIdShould {

    @Test
    public void not_accept_nulls() {
        new NullPointerTester()
                .setDefault(Datastore.class, mockDatastore())
                .testStaticMethods(ProjectId.class, NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    public void support_equality() {
        final ProjectId a1 = ProjectId.of("a");
        final ProjectId a2 = ProjectId.of(mockDatastore("a"));

        final ProjectId b1 = ProjectId.of("b");
        final ProjectId b2 = ProjectId.of(mockDatastore("b"));

        new EqualsTester()
                .addEqualityGroup(a1, a2)
                .addEqualityGroup(b1, b2)
                .testEquals();
    }

    @Test
    public void support_toString() {
        final String value = "my-fancy-project-id";
        final ProjectId projectId = ProjectId.of(value);
        final String stringRepr = projectId.toString();
        assertThat(stringRepr, containsString(value));
    }

    private static Datastore mockDatastore() {
        return mockDatastore("some-project-id-ProjectIdShould");
    }

    private static Datastore mockDatastore(String value) {
        final Datastore datastore = mock(Datastore.class);
        final DatastoreOptions options = mock(DatastoreOptions.class);
        when(datastore.getOptions()).thenReturn(options);
        when(options.getProjectId()).thenReturn(value);
        return datastore;
    }
}
