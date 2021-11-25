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

package io.spine.server.storage.datastore.tenant;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.common.testing.NullPointerTester;
import io.spine.base.Identifier;
import io.spine.core.TenantId;
import io.spine.environment.Tests;
import io.spine.net.InternetDomain;
import io.spine.server.BoundedContext;
import io.spine.server.BoundedContextBuilder;
import io.spine.server.ServerEnvironment;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordSpec;
import io.spine.server.storage.datastore.DatastoreStorageFactory;
import io.spine.server.storage.datastore.tenant.given.CollegeProjection;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.testing.TestValues;
import io.spine.testing.server.storage.datastore.TestDatastores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.server.tenant.TenantAwareRunner.with;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@DisplayName("`NamespaceIndex` should")
final class NamespaceIndexTest {

    private static final NsConverterFactory converterFactory = NsConverterFactory.defaults();

    private NamespaceIndex namespaceIndex;
    private BoundedContext context;

    private static TenantId newTenantId() {
        return TenantId.newBuilder()
                .setValue(TestValues.randomString())
                .vBuild();
    }

    @BeforeEach
    void createIndex() {
        namespaceIndex = nsIndex();
        context = BoundedContextBuilder.assumingTests()
                                       .build();
    }

    @AfterEach
    void closeContext() throws Exception {
        context.close();
    }

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void testNulls() {
        var defaultNamespace = Namespace.of("some-string");
        var tenantId = TenantId.getDefaultInstance();

        new NullPointerTester()
                .setDefault(Namespace.class, defaultNamespace)
                .setDefault(TenantId.class, tenantId)
                .setDefault(BoundedContext.class, context)
                .testInstanceMethods(namespaceIndex, NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    @DisplayName("store tenant IDs")
    void testStore() {
        var existingIds = namespaceIndex.all();
        var idCount = existingIds.size();

        var newId = newTenantId();
        namespaceIndex.keep(newId);

        var ids = namespaceIndex.all();
        var assertIds = assertThat(ids);
        assertIds.isNotNull();
        assertIds.hasSize(idCount + 1);

        assertThat(ids).contains(newId);
    }

    @Test
    @DisplayName("do nothing on close")
    void testClose() {
        namespaceIndex.close();
        namespaceIndex.close();
        // No exception is thrown on the second call to #close() => no operation is performed
    }

    @Test
    @DisplayName("find existing namespaces")
    void testFindExisting() {
        var newId = newTenantId();
        var newNamespace = Namespace.of(newId, true);

        namespaceIndex.keep(newId);
        assertTrue(namespaceIndex.contains(newNamespace));
    }

    @Test
    @DisplayName("not find non-existing namespaces")
    void testNotFindNonExisting() {
        var fakeId = newTenantId();
        var fakeNamespace = Namespace.of(fakeId, true);

        assertFalse(namespaceIndex.contains(fakeNamespace));
    }

    @Test
    @DisplayName("find tenants by prefixed namespaces")
    void findPrefixedNamespaces() {
        var datastore = TestDatastores
                .local()
                .getOptions()
                .toBuilder()
                .setNamespace("Vcustom-namespace")
                .build()
                .getService();
        var contextBuilder = BoundedContextBuilder
                .assumingTests(true);
        var storageFactory = DatastoreStorageFactory.newBuilder()
                .setDatastore(datastore)
                .build();
        ServerEnvironment.when(Tests.class)
                         .use(storageFactory);
        storageFactory.configureTenantIndex(contextBuilder);
        var context = contextBuilder.build();
        var storage = storageFactory
                .createRecordStorage(context.spec(), EntityRecordSpec.of(CollegeProjection.class));
        var id = CollegeId.newBuilder()
                .setValue("Aeronautic Forgery College")
                .vBuild();
        var record = EntityRecord.newBuilder()
                .setEntityId(Identifier.pack(id))
                .setState(pack(College.newBuilder()
                                       .setId(id)
                                       .setName(id.getValue())
                                       .build()))
                .build();
        var tenantId = TenantId.newBuilder()
                .setValue("XYZ")
                .build();
        with(tenantId).run(
                () -> storage.write(id, record)
        );
        var tenantIds = context.internalAccess()
                               .tenantIndex()
                               .all();
        assertThat(tenantIds).containsExactly(tenantId);
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
        keys.add(key("Vtenant1"));
        keys.add(key("Vtenant2"));
        keys.add(key("Vtenant3"));
        Collection<TenantId> initialTenantIds =
                keys.stream()
                        .map(key -> TenantId.newBuilder()
                                .setValue(key.getName()
                                             .substring(1))
                                .vBuild())
                        .collect(toList());

        NamespaceIndex.NamespaceQuery namespaceQuery = keys::iterator;
        // The tested object
        var namespaceIndex = nsIndexFor(namespaceQuery);

        // The test flow
        Runnable flow = () -> {
            // Initial value check
            var initialIdsActual = namespaceIndex.all(); // sync
            // The keep may already be called
            assertTrue(initialIdsActual.size() >= initialTenantIds.size());
            assertThat(initialIdsActual).containsAtLeastElementsIn(initialTenantIds);

            // Add new element
            var domain = InternetDomain.newBuilder()
                    .setValue("my.tenant.com")
                    .vBuild();
            var newTenantId = TenantId.newBuilder()
                    .setDomain(domain)
                    .vBuild();
            namespaceIndex.keep(newTenantId); // sync

            // Check new value added
            var success = namespaceIndex.contains(Namespace.of(newTenantId,    // sync
                                                               true));
            assertTrue(success);

            // Check returned set has newly added element
            var updatedIds = namespaceIndex.all(); // sync
            assertEquals(updatedIds.size(), initialTenantIds.size() + 1);
            assertThat(updatedIds).contains(newTenantId);
        };

        // Test execution threads
        var firstThread = new Thread(flow);
        var secondThread = new Thread(flow);

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
        for (var failure : threadFailures.values()) {
            fail(format("Test thread has thrown a Throwable. %s",
                        getStackTraceAsString(failure)));
        }
    }

    private static NamespaceIndex nsIndexFor(NamespaceIndex.NamespaceQuery namespaceQuery) {
        return new NamespaceIndex(namespaceQuery, true, converterFactory);
    }

    private static NamespaceIndex nsIndex() {
        return new NamespaceIndex(datastore(), true, converterFactory);
    }

    private static Key key(String name) {
        var key = Key.newBuilder("some-proj", "some-kind", name)
                .build();
        return key;
    }

    private static Datastore datastore() {
        var namespace = "Vsome-namespace";
        var options = TestDatastores
                .local()
                .getOptions()
                .toBuilder()
                .setNamespace(namespace)
                .build();
        var datastore = options.getService();
        return datastore;
    }
}
