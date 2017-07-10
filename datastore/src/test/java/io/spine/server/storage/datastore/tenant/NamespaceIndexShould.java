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

package io.spine.server.storage.datastore.tenant;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.testing.NullPointerTester;
import io.spine.net.InternetDomain;
import io.spine.server.storage.datastore.given.Given;
import io.spine.server.storage.datastore.ProjectId;
import io.spine.core.TenantId;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Collections2.transform;
import static io.spine.server.storage.datastore.given.Given.TEST_PROJECT_ID;
import static io.spine.test.Verify.assertContains;
import static io.spine.test.Verify.assertContainsAll;
import static io.spine.test.Verify.assertSize;
import static java.lang.String.format;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class NamespaceIndexShould {

    private static final String TENANT_ID_STRING = "some-tenant";

    @Test
    public void not_accept_nulls() {
        final Namespace defaultNamespace = Namespace.of("some-string");
        final TenantId tenantId = TenantId.getDefaultInstance();

        new NullPointerTester()
                .setDefault(Namespace.class, defaultNamespace)
                .setDefault(TenantId.class, tenantId)
                .testInstanceMethods(new NamespaceIndex(mockDatastore(), true),
                                     NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    public void store_tenant_ids() {
        final NamespaceIndex namespaceIndex = new NamespaceIndex(mockDatastore(), true);

        final Set<TenantId> initialEmptySet = namespaceIndex.getAll();
        assertTrue(initialEmptySet.isEmpty());

        final TenantId newId = TenantId.newBuilder()
                                       .setValue(TENANT_ID_STRING)
                                       .build();
        namespaceIndex.keep(newId);

        final Set<TenantId> ids = namespaceIndex.getAll();
        assertFalse(ids.isEmpty());
        assertSize(1, ids);

        final TenantId actual = ids.iterator()
                                   .next();
        assertEquals(newId, actual);
    }

    @Test
    public void do_nothing_on_close() {
        final NamespaceIndex namespaceIndex = new NamespaceIndex(mockDatastore(), false);

        namespaceIndex.close();
        namespaceIndex.close();
        // No exception is thrown on the second call to #close() => no operation is performed
    }

    @Test
    public void find_existing_namespaces() {
        final NamespaceIndex namespaceIndex = new NamespaceIndex(mockDatastore(), true);

        // Ensure no namespace has been kept
        final Set<TenantId> initialEmptySet = namespaceIndex.getAll();
        assertTrue(initialEmptySet.isEmpty());

        final TenantId newId = TenantId.newBuilder()
                                       .setValue(TENANT_ID_STRING)
                                       .build();
        final Namespace newNamespace = Namespace.of(newId, Given.TEST_PROJECT_ID);

        namespaceIndex.keep(newId);
        assertTrue(namespaceIndex.contains(newNamespace));
    }

    @Test
    public void not_find_non_existing_namespaces() {
        final NamespaceIndex namespaceIndex = new NamespaceIndex(mockDatastore(), true);

        // Ensure no namespace has been kept
        final Set<TenantId> initialEmptySet = namespaceIndex.getAll();
        assertTrue(initialEmptySet.isEmpty());

        final TenantId fakeId = TenantId.newBuilder()
                                        .setValue(TENANT_ID_STRING)
                                        .build();
        final Namespace fakeNamespace = Namespace.of(fakeId, ProjectId.of("fake-prj"));

        assertFalse(namespaceIndex.contains(fakeNamespace));
    }

    @Test(timeout = 5000L) // 5 second execution indicates a possible dead lock
    public void synchronize_access_methods() throws InterruptedException {
        // Initial data
        final Collection<Key> keys = new LinkedList<>();
        keys.add(mockKey("Vtenant1"));
        keys.add(mockKey("Vtenant2"));
        keys.add(mockKey("Vtenant3"));
        final Function<Key, TenantId> keyTenantIdTransformer =
                new Function<Key, TenantId>() {
                    @Override
                    public TenantId apply(@Nullable Key input) {
                        assertNotNull(input);
                        return TenantId.newBuilder()
                                       .setValue(input.getName()
                                                      .substring(1))
                                       .build();
                    }
                };
        final Collection<TenantId> initialTenantIds = transform(keys, keyTenantIdTransformer);

        final NamespaceIndex.NamespaceQuery namespaceQuery = new NamespaceIndex.NamespaceQuery() {
            @Override
            public Iterator<Key> run() {
                return keys.iterator();
            }
        };
        // The tested object
        final NamespaceIndex namespaceIndex = new NamespaceIndex(namespaceQuery,
                                                                 TEST_PROJECT_ID,
                                                                 true);

        // The test flow
        final Runnable flow = new Runnable() {
            @Override
            public void run() {
                // Initial value check
                final Set<TenantId> initialIdsActual = namespaceIndex.getAll(); // sync
                // The keep may already be called
                assertThat(initialIdsActual.size(), greaterThanOrEqualTo(initialTenantIds.size()));
                @SuppressWarnings("ZeroLengthArrayAllocation")
                final TenantId[] elements = initialTenantIds.toArray(new TenantId[0]);
                assertContainsAll(initialIdsActual, elements);

                // Add new element
                final InternetDomain domain = InternetDomain.newBuilder()
                                                            .setValue("my.tenant.com")
                                                            .build();
                final TenantId newTenantId = TenantId.newBuilder()
                                                     .setDomain(domain)
                                                     .build();
                namespaceIndex.keep(newTenantId); // sync

                // Check new value added
                final boolean success = namespaceIndex.contains(Namespace.of(newTenantId,    // sync
                                                                             TEST_PROJECT_ID));
                assertTrue(success);

                // Check returned set has newly added element
                final Set<TenantId> updatedIds = namespaceIndex.getAll(); // sync
                assertEquals(updatedIds.size(), initialTenantIds.size() + 1);
                assertContains(newTenantId, updatedIds);
            }
        };

        // Test execution threads
        final Thread firstThread = new Thread(flow);
        final Thread secondThread = new Thread(flow);

        // Collect thread failures
        final Map<Thread, Throwable> threadFailures = new HashMap<>(2);
        final Thread.UncaughtExceptionHandler throwableCollector =
                new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        threadFailures.put(t, e);
                    }
                };

        firstThread.setUncaughtExceptionHandler(throwableCollector);
        secondThread.setUncaughtExceptionHandler(throwableCollector);

        // Start parallel execution
        firstThread.start();
        secondThread.start();

        // Await both threads to complete
        firstThread.join();
        secondThread.join();

        // Check for failures
        // Throw if any, failing the test
        for (Throwable failure : threadFailures.values()) {
            fail(format("Test thread has thrown a Throwable. %s",
                        Throwables.getStackTraceAsString(failure)));
        }
    }

    private static Key mockKey(String name) {
        final Key key = Key.newBuilder("some-proj", "some-kind", name)
                           .build();
        return key;
    }

    private static Datastore mockDatastore() {
        final Datastore datastore = mock(Datastore.class);
        final DatastoreOptions options = mock(DatastoreOptions.class);
        when(datastore.getOptions()).thenReturn(options);
        when(options.getProjectId()).thenReturn("some-project-id-NamespaceIndexShould");
        final QueryResults results = mock(QueryResults.class);
        when(datastore.run(any(Query.class))).thenReturn(results);
        return datastore;
    }
}
