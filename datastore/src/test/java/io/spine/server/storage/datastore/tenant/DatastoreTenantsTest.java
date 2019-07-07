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

package io.spine.server.storage.datastore.tenant;

import com.google.cloud.datastore.Cursor;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.common.collect.ImmutableList;
import io.spine.core.TenantId;
import io.spine.server.tenant.TenantIndex;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("`DatastoreTenants` should")
class DatastoreTenantsTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void have_private_utility_ctor() {
        assertHasPrivateParameterlessCtor(DatastoreTenants.class);
    }

    @Test
    @DisplayName("create tenant index")
    void testCreateIndex() {
        TenantIndex index = DatastoreTenants.index(mockDatastore());
        assertNotNull(index);
        assertThat(index).isInstanceOf(NamespaceIndex.class);

        String customNamespace = "my-namespace";
        TenantId customId = TenantId
                .newBuilder()
                .setValue(customNamespace)
                .vBuild();
        index.keep(customId);
        Set<TenantId> ids = index.all();
        assertThat(ids).contains(customId);
    }

    @SuppressWarnings("unchecked") // Mocking.
    private static Datastore mockDatastore() {
        Datastore datastore = mock(Datastore.class);
        DatastoreOptions options = mock(DatastoreOptions.class);
        when(datastore.getOptions()).thenReturn(options);
        when(options.getProjectId()).thenReturn("some-project-id-DatastoreTenantsTest");
        when(datastore.run(any(Query.class))).thenReturn(new MockKeyQueryResults());
        return datastore;
    }

    @SuppressWarnings("NewExceptionWithoutArguments")
    private static class MockKeyQueryResults implements QueryResults<Key> {

        private static final List<Key> keys =
                ImmutableList.of(mockKey("Vfoo"), mockKey("Vbar"), mockKey("Vbaz"));

        private final Iterator<Key> keyIterator = keys.iterator();

        private static Key mockKey(String name) {
            Key key = Key.newBuilder("my-proj", "my-kind", name)
                         .build();
            return key;
        }

        @Override
        public Class<?> getResultClass() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor getCursorAfter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSkippedResults() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            return keyIterator.hasNext();
        }

        @Override
        public Key next() {
            return keyIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
