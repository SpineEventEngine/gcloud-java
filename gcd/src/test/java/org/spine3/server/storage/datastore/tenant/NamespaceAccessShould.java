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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import org.junit.Test;
import org.spine3.users.TenantId;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.spine3.test.Verify.assertSize;

/**
 * @author Dmytro Dashenkov
 */
public class NamespaceAccessShould {

    private static final String TENANT_ID_STRING = "some-tenant";

    @Test
    public void store_tenant_ids() {
        final NamespaceAccess namespaceAccess = new NamespaceAccess(mockDatastore());

        final Set<TenantId> initialEmptySet = namespaceAccess.getAll();
        assertTrue(initialEmptySet.isEmpty());

        final TenantId newId = TenantId.newBuilder()
                                       .setValue(TENANT_ID_STRING)
                                       .build();
        namespaceAccess.keep(newId);

        final Set<TenantId> ids = namespaceAccess.getAll();
        assertFalse(ids.isEmpty());
        assertSize(1, ids);

        final TenantId actual = ids.iterator().next();
        assertEquals(newId, actual);
    }

    @Test
    public void do_nothing_on_close() {
        final NamespaceAccess namespaceAccess = new NamespaceAccess(mockDatastore());

        namespaceAccess.close();
        namespaceAccess.close();
        // No exception is thrown on the second call to #close() => no operation is performed
    }

    @Test
    public void find_existing_namespaces() {
        final NamespaceAccess namespaceAccess = new NamespaceAccess(mockDatastore());

        // Ensure no namespace has been kept
        final Set<TenantId> initialEmptySet = namespaceAccess.getAll();
        assertTrue(initialEmptySet.isEmpty());

        final TenantId newId = TenantId.newBuilder()
                                       .setValue(TENANT_ID_STRING)
                                       .build();
        final Namespace newNamespace = Namespace.of(newId);

        namespaceAccess.keep(newId);
        assertTrue(namespaceAccess.exists(newNamespace));
    }

    @Test
    public void not_find_non_existing_namespaces() {
        final NamespaceAccess namespaceAccess = new NamespaceAccess(mockDatastore());

        // Ensure no namespace has been kept
        final Set<TenantId> initialEmptySet = namespaceAccess.getAll();
        assertTrue(initialEmptySet.isEmpty());

        final TenantId fakeId = TenantId.newBuilder()
                                       .setValue(TENANT_ID_STRING)
                                       .build();
        final Namespace fakeNamespace = Namespace.of(fakeId);

        assertFalse(namespaceAccess.exists(fakeNamespace));
    }

    private static Datastore mockDatastore() {
        final Datastore datastore = mock(Datastore.class);
        final QueryResults results = mock(QueryResults.class);
        when(datastore.run(any(Query.class))).thenReturn(results);
        return datastore;
    }
}
