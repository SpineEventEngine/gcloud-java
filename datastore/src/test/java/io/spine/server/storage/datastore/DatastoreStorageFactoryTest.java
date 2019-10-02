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
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.testing.NullPointerTester;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import io.spine.base.Identifier;
import io.spine.base.Time;
import io.spine.core.TenantId;
import io.spine.server.ContextSpec;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.given.TestEnvironment;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import io.spine.test.storage.Project;
import io.spine.type.TypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.ContextSpec.multitenant;
import static io.spine.server.tenant.TenantAwareRunner.with;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.testing.server.storage.datastore.TestDatastores.local;
import static io.spine.testing.server.storage.datastore.TestDatastores.defaultLocalProjectId;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("`DatastoreStorageFactory` should")
class DatastoreStorageFactoryTest {

    private static final DatastoreOptions DUMMY_OPTIONS =
            DatastoreOptions.newBuilder()
                            .setProjectId("dummy-dataset")
                            .build();

    private static final Datastore datastore = DUMMY_OPTIONS.getService();

    private static final StorageFactory factory =
            DatastoreStorageFactory.newBuilder()
                                   .setDatastore(datastore)
                                   .build();

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void testNulls() {
        new NullPointerTester().setDefault(ContextSpec.class, TestEnvironment.singleTenantSpec())
                               .testAllPublicInstanceMethods(factory);
    }

    @Test
    @DisplayName("create multitenant storages")
    void testCreateMultitenant() {
        StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                        .setDatastore(datastore)
                                                        .build();
        RecordStorage storage =
                factory.createRecordStorage(TestEnvironment.multiTenantSpec(), TestEntity.class);
        assertTrue(storage.isMultitenant());
        storage.close();
    }

    @Test
    @DisplayName("create separate record storage per state type")
    void testDependsOnStateType() {
        ContextSpec spec = TestEnvironment.singleTenantSpec();
        DsRecordStorage<?> storage =
                (DsRecordStorage<?>) factory.createRecordStorage(spec, TestEntity.class);
        assertNotNull(storage);
        DsRecordStorage<?> differentStorage =
                (DsRecordStorage<?>) factory.createRecordStorage(spec, DifferentTestEntity.class);
        assertNotNull(differentStorage);
        assertNotEquals(storage.getKind(), differentStorage.getKind());
    }

    @Test
    @DisplayName("have default column type registry")
    void testDefaultColumnTypeRegistry() {
        DatastoreStorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                                 .setDatastore(datastore)
                                                                 .build();
        ColumnTypeRegistry defaultRegistry = factory.getTypeRegistry();
        assertNotNull(defaultRegistry);
        assertSame(DatastoreTypeRegistryFactory.defaultInstance(), defaultRegistry);
    }

    @Test
    @DisplayName("do nothing on close")
    void testClose() {
        DatastoreStorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                                 .setDatastore(datastore)
                                                                 .build();
        factory.close();
        // Multiple calls are allowed as no action is performed
        factory.close();
    }

    @Test
    @DisplayName("allow custom namespaces for multitenant storages")
    void namespaceForMultitenant() {
        TenantId tenant = TenantId
                .newBuilder()
                .setValue("my-company")
                .vBuild();
        String namespace = "Vnon-null-or-empty-namespace";
        DatastoreOptions options = local().getOptions()
                                          .toBuilder()
                                          .setNamespace(namespace)
                                          .build();
        ContextSpec spec =
                multitenant(DatastoreStorageFactoryBuilderTest.class.getSimpleName());
        Datastore datastore = options.getService();
        DatastoreStorageFactory factory = DatastoreStorageFactory
                .newBuilder()
                .setDatastore(datastore)
                .build();
        DsPropertyStorage storage = factory.createPropertyStorage(spec);
        Key key = whiteForTenant(storage, tenant)
                .setNamespace(namespace + ".V" + tenant.getValue())
                .build();
        Entity entity = datastore.get(key);
        assertThat(entity).isNotNull();
    }

    @Test
    @DisplayName("allow no custom namespaces for multitenant storages")
    void testDatastoreNamespaceInOptions() {
        TenantId tenant = TenantId
                .newBuilder()
                .setValue("your-company")
                .vBuild();
        ContextSpec spec =
                multitenant(DatastoreStorageFactoryBuilderTest.class.getSimpleName());
        Datastore datastore = local();
        DatastoreStorageFactory factory = DatastoreStorageFactory
                .newBuilder()
                .setDatastore(datastore)
                .build();
        DsPropertyStorage storage = factory.createPropertyStorage(spec);
        Key key = whiteForTenant(storage, tenant)
                .setNamespace('V' + tenant.getValue())
                .build();
        Entity entity = datastore.get(key);
        assertThat(entity).isNotNull();
    }

    private static Key.Builder whiteForTenant(DsPropertyStorage storage, TenantId tenant) {
        RecordId recordId = RecordId.of(Identifier.newUuid());
        Timestamp message = Time.currentTime();
        with(tenant).run(
                () -> storage.write(recordId, message)
        );
        return Key.newBuilder(defaultLocalProjectId().getValue(),
                              TypeName.of(message)
                                      .value(),
                              recordId.getValue());
    }

    private static class TestEntity extends AbstractEntity<String, StringValue> {

        private TestEntity(String id) {
            super(id);
        }
    }

    private static class DifferentTestEntity extends AbstractEntity<String, Project> {

        protected DifferentTestEntity(String id) {
            super(id);
        }
    }
}
