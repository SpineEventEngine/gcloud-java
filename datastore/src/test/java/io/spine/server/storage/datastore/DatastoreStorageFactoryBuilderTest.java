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

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Value;
import com.google.common.testing.NullPointerTester;
import com.google.protobuf.Timestamp;
import io.spine.base.Identifier;
import io.spine.base.Time;
import io.spine.core.TenantId;
import io.spine.server.ContextSpec;
import io.spine.server.entity.storage.ColumnType;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.datastore.given.TestDatastores;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import io.spine.server.storage.datastore.type.SimpleDatastoreColumnType;
import io.spine.server.tenant.TenantAwareRunner;
import io.spine.type.TypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.ContextSpec.multitenant;
import static io.spine.server.ContextSpec.singleTenant;
import static io.spine.server.storage.datastore.given.TestDatastores.projectId;
import static io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory.predefinedValuesAnd;
import static io.spine.server.tenant.TenantAwareRunner.with;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static io.spine.testing.Tests.nullRef;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("DatastoreStorageFactory.Builder should")
class DatastoreStorageFactoryBuilderTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void testPrivateCtor() {
        assertHasPrivateParameterlessCtor(DatastoreStorageFactory.Builder.class);
    }

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void testNulls() {
        new NullPointerTester()
                .setDefault(Datastore.class,
                            mockDatastore())
                .setDefault(ColumnTypeRegistry.class,
                            DatastoreTypeRegistryFactory.defaultInstance())
                .testInstanceMethods(DatastoreStorageFactory.newBuilder(),
                                     NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    @DisplayName("construct factories with default type registry")
    void testDefaultTypeRegistry() {
        DatastoreStorageFactory factory = DatastoreStorageFactory
                .newBuilder()
                .setDatastore(mockDatastore())
                .build();
        ColumnTypeRegistry registry = factory.getTypeRegistry();
        assertNotNull(registry);
    }

    @Test
    @DisplayName("construct factories with extended type registry")
    void testExtendedTypeRegistry() {
        DatastoreStorageFactory factory =
                DatastoreStorageFactory.newBuilder()
                                       .setDatastore(mockDatastore())
                                       .setTypeRegistry(predefinedValuesAnd()
                                                                .put(Byte.class,
                                                                     new MockByteColumnType())
                                                                .build())
                                       .build();
        ColumnTypeRegistry<?> registry = factory.getTypeRegistry();
        assertNotNull(registry);
        ColumnType type = registry.get(mockColumn(Byte.class));
        assertNotNull(type);
    }

    @Nested
    class Namespaces {

        private DatastoreOptions.Builder builder;

        @BeforeEach
        void setUp() {
            builder = DatastoreOptions
                    .newBuilder()
                    .setProjectId(projectId().getValue());
        }

        @Test
        @DisplayName("allow custom namespace for single tenant instances")
        void testCustomNamespace() {
            String namespace = "my.custom.namespace";
            DatastoreOptions options =
                    builder.setNamespace(namespace)
                           .build();
            ContextSpec spec =
                    singleTenant(DatastoreStorageFactoryBuilderTest.class.getSimpleName());
            DatastoreStorageFactory factory = DatastoreStorageFactory
                    .newBuilder()
                    .setDatastore(options.getService())
                    .build();
            assertNotNull(factory);
            String actualNamespace = factory.wrapperFor(spec)
                                            .datastoreOptions()
                                            .getNamespace();
            assertEquals(namespace, actualNamespace);
        }
    }

    @Test
    @DisplayName("fail to construct without Datastore")
    void testRequireDatastore() {
        assertThrows(NullPointerException.class, DatastoreStorageFactory.newBuilder()::build);
    }

    private static Datastore mockDatastore() {
        return TestDatastores.local();
    }

    private static <T> EntityColumn mockColumn(Class<T> type) {
        EntityColumn mock = mock(EntityColumn.class);
        when(mock.type()).thenReturn(type);
        when(mock.persistedType()).thenReturn(type);
        return mock;
    }

    private static class MockByteColumnType extends SimpleDatastoreColumnType<Byte> {

        @Override
        public void setColumnValue(BaseEntity.Builder storageRecord, Byte value,
                                   String columnIdentifier) {
            // NOP
        }

        @Override
        public Value<?> toValue(Byte data) {
            // NOP
            return nullRef();
        }
    }
}
