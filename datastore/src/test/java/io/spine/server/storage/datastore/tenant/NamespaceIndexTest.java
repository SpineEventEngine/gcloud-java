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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.common.testing.NullPointerTester;
import com.google.common.truth.IterableSubject;
import io.spine.core.TenantId;
import io.spine.net.InternetDomain;
import io.spine.server.storage.datastore.ProjectId;
import io.spine.server.storage.datastore.given.TestDatastores;
import io.spine.testing.TestValues;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("NamespaceIndex should")
class NamespaceIndexTest {

    private static TenantId newTenantId() {
        return TenantId.newBuilder()
                       .setValue(TestValues.randomString())
                       .build();
    }

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void testNulls() {
        Namespace defaultNamespace = Namespace.of("some-string");
        TenantId tenantId = TenantId.getDefaultInstance();

        new NullPointerTester()
                .setDefault(Namespace.class, defaultNamespace)
                .setDefault(TenantId.class, tenantId)
                .testInstanceMethods(new NamespaceIndex(mockDatastore(), true),
                                     NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    @DisplayName("store tenant IDs")
    void testStore() {
        NamespaceIndex namespaceIndex = new NamespaceIndex(mockDatastore(), true);

        Set<TenantId> initialEmptySet = namespaceIndex.getAll();
        assertTrue(initialEmptySet.isEmpty());

        TenantId newId = newTenantId();
        namespaceIndex.keep(newId);

        Set<TenantId> ids = namespaceIndex.getAll();
        IterableSubject assertIds = assertThat(ids);
        assertIds.isNotNull();
        assertIds.hasSize(1);

        TenantId actual = ids.iterator()
                             .next();
        assertEquals(newId, actual);
    }

    @Test
    @DisplayName("do nothing on close")
    void testClose() {
        NamespaceIndex namespaceIndex = new NamespaceIndex(mockDatastore(), false);

        namespaceIndex.close();
        namespaceIndex.close();
        // No exception is thrown on the second call to #close() => no operation is performed
    }

    @Test
    @DisplayName("find existing namespaces")
    void testFindExisting() {
        NamespaceIndex namespaceIndex = new NamespaceIndex(mockDatastore(), true);

        // Ensure no namespace has been kept
        Set<TenantId> initialEmptySet = namespaceIndex.getAll();
        assertTrue(initialEmptySet.isEmpty());

        TenantId newId = newTenantId();
        Namespace newNamespace = Namespace.of(newId, TestDatastores.projectId());

        namespaceIndex.keep(newId);
        assertTrue(namespaceIndex.contains(newNamespace));
    }

    @Test
    @DisplayName("not find non existing namespaces")
    void testNotFindNonExisting() {
        NamespaceIndex namespaceIndex = new NamespaceIndex(mockDatastore(), true);

        // Ensure no namespace has been kept
        Set<TenantId> initialEmptySet = namespaceIndex.getAll();
        assertTrue(initialEmptySet.isEmpty());

        TenantId fakeId = newTenantId();
        Namespace fakeNamespace = Namespace.of(fakeId, ProjectId.of("fake-prj"));

        assertFalse(namespaceIndex.contains(fakeNamespace));
    }

    @Test
    @DisplayName("synchronize access methods")
    void testAsync() {
        assertTimeout(Duration.ofSeconds(5L),
                      NamespaceIndexTest::testSynchronizeAccessMethods);
    }

    @SuppressWarnings("OverlyLongMethod")
    private static void testSynchronizeAccessMethods() throws InterruptedException {
        // Initial data
        Collection<Key> keys = new ArrayList<>();
        keys.add(mockKey("Vtenant1"));
        keys.add(mockKey("Vtenant2"));
        keys.add(mockKey("Vtenant3"));
        Collection<TenantId> initialTenantIds =
                keys.stream()
                    .map(key -> TenantId.newBuilder()
                                        .setValue(key.getName().substring(1))
                                        .build())
                    .collect(toList());

        NamespaceIndex.NamespaceQuery namespaceQuery = keys::iterator;
        // The tested object
        NamespaceIndex namespaceIndex = new NamespaceIndex(namespaceQuery,
                                                           TestDatastores.projectId(),
                                                           true);

        // The test flow
        Runnable flow = () -> {
            // Initial value check
            Set<TenantId> initialIdsActual = namespaceIndex.getAll(); // sync
            // The keep may already be called
            assertTrue(initialIdsActual.size() >= initialTenantIds.size());
            assertThat(initialIdsActual).containsAllIn(initialTenantIds);

            // Add new element
            InternetDomain domain = InternetDomain
                    .newBuilder()
                    .setValue("my.tenant.com")
                    .build();
            TenantId newTenantId = TenantId
                    .newBuilder()
                    .setDomain(domain)
                    .build();
            namespaceIndex.keep(newTenantId); // sync

            // Check new value added
            boolean success = namespaceIndex.contains(Namespace.of(newTenantId,    // sync
                                                                   TestDatastores.projectId()));
            assertTrue(success);

            // Check returned set has newly added element
            Set<TenantId> updatedIds = namespaceIndex.getAll(); // sync
            assertEquals(updatedIds.size(), initialTenantIds.size() + 1);
            assertThat(updatedIds).contains(newTenantId);
        };

        // Test execution threads
        Thread firstThread = new Thread(flow);
        Thread secondThread = new Thread(flow);

        // Collect thread failures
        Map<Thread, Throwable> threadFailures = new HashMap<>(2);
        Thread.UncaughtExceptionHandler throwableCollector = threadFailures::put;

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
                        getStackTraceAsString(failure)));
        }
    }

    private static Key mockKey(String name) {
        Key key = Key.newBuilder("some-proj", "some-kind", name)
                     .build();
        return key;
    }

    @SuppressWarnings("unchecked") // OK for mocking in this method.
    private static Datastore mockDatastore() {
        Datastore datastore = mock(Datastore.class);
        DatastoreOptions options = mock(DatastoreOptions.class);
        when(datastore.getOptions()).thenReturn(options);
        when(options.getProjectId()).thenReturn("some-project-id-NamespaceIndexTest");
        QueryResults results = mock(QueryResults.class);
        when(datastore.run(any(Query.class))).thenReturn(results);
        return datastore;
    }
}
