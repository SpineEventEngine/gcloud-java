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

package org.spine3.server.storage.datastore.tenant;

import com.google.cloud.datastore.Cursor;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import org.junit.Test;
import org.spine3.server.tenant.TenantIndex;
import org.spine3.users.TenantId;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.spine3.test.Verify.assertContains;

/**
 * @author Dmytro Dashenkov
 */
public class DatastoreTenantsShould {

    @Test
    public void create_tenant_index() {
        final TenantIndex index = DatastoreTenants.index(mockDatastore());
        assertNotNull(index);
        assertThat(index, instanceOf(NamespaceAccess.class));

        final String customNamespace = "my-namespace";
        final TenantId customId = TenantId.newBuilder()
                                          .setValue(customNamespace)
                                          .build();
        index.eep(customId);
        final Set<TenantId> ids = index.getAll();
        assertContains(customId, ids);
    }

    private static Datastore mockDatastore() {
        final Datastore datastore = mock(Datastore.class);
        when(datastore.run(any(Query.class))).thenReturn(new MockKeyQueryResults());
        return datastore;
    }

    private static Key mockKey(String name) {
        final Key key = Key.newBuilder("my-proj", "my-kind", name)
                           .build();
        return key;
    }

    private static class MockKeyQueryResults implements QueryResults<Key> {

        @SuppressWarnings({"serial", "ClassExtendsConcreteCollection"}) // For test purposes
        private static final List<Key> keys = new LinkedList<Key>() {{
            add(mockKey("foo"));
            add(mockKey("bar"));
            add(mockKey("baz"));
        }};
        private final Iterator<Key> keyIterator = keys.iterator();

        @Override
        public Class<?> resultClass() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Class<?> getResultClass() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor cursorAfter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor getCursorAfter() {
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
