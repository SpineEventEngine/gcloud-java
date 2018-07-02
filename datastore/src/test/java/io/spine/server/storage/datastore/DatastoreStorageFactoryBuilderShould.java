/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import com.google.cloud.datastore.Value;
import com.google.common.testing.NullPointerTester;
import io.spine.server.entity.storage.ColumnType;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.tenant.NamespaceToTenantIdConverter;
import io.spine.server.storage.datastore.tenant.TenantConverterRegistry;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import io.spine.server.storage.datastore.type.SimpleDatastoreColumnType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory.predefinedValuesAnd;
import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static io.spine.test.Tests.nullRef;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class DatastoreStorageFactoryBuilderShould {

    @Test
    public void have_private_ctor() {
        assertHasPrivateParameterlessCtor(DatastoreStorageFactory.Builder.class);
    }

    @Test
    public void not_accept_nulls() {
        new NullPointerTester()
                .setDefault(Datastore.class, mockDatastore())
                .setDefault(ColumnTypeRegistry.class, DatastoreTypeRegistryFactory.defaultInstance())
                .testInstanceMethods(DatastoreStorageFactory.newBuilder(), NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    public void construct_factories_with_default_type_registry() {
        StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                              .setDatastore(mockDatastore())
                                                              .build();
        ColumnTypeRegistry registry = factory.getTypeRegistry();
        assertNotNull(registry);
    }

    @Test
    public void construct_single_tenant_factories_by_default() {
        StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                              .setDatastore(mockDatastore())
                                                              .build();
        assertFalse(factory.isMultitenant());
    }

    @Test
    public void construct_factories_with_extended_type_registry() {
        StorageFactory factory =
                DatastoreStorageFactory.newBuilder()
                                       .setDatastore(mockDatastore())
                                       .setTypeRegistry(predefinedValuesAnd()
                                                                .put(Byte.class, new MockByteColumnType())
                                                                .build())
                                       .build();
        ColumnTypeRegistry<?> registry = factory.getTypeRegistry();
        assertNotNull(registry);
        ColumnType type = registry.get(mockColumn(Byte.class));
        assertNotNull(type);
    }

    @Test
    public void ensure_datastore_has_no_namespace_if_multitenant() {
        DatastoreOptions options =
                DatastoreOptions.newBuilder()
                                .setNamespace("non-null-or-empty-namespace")
                                .setProjectId(TestDatastoreStorageFactory.DEFAULT_DATASET_NAME)
                                .build();
        DatastoreStorageFactory.Builder builder = DatastoreStorageFactory
                .newBuilder()
                .setMultitenant(true)
                .setDatastore(options.getService());
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void allow_custom_namespace_for_single_tenant_instances() {
        String namespace = "my.custom.namespace";
        DatastoreOptions options =
                DatastoreOptions.newBuilder()
                                .setProjectId(TestDatastoreStorageFactory.DEFAULT_DATASET_NAME)
                                .setNamespace(namespace)
                                .build();
        DatastoreStorageFactory factory =
                DatastoreStorageFactory.newBuilder()
                                       .setMultitenant(false)
                                       .setDatastore(options.getService())
                                       .build();
        assertNotNull(factory);
        String actualNamespace = factory.getDatastore()
                                              .getDatastoreOptions()
                                              .getNamespace();
        assertEquals(namespace, actualNamespace);
    }

    @Test
    public void register_custom_tenant_id_converter_upon_build() {
        ProjectId withCustomConverter = ProjectId.of("customized");
        ProjectId withDefaultConverter = ProjectId.of("defaulted");

        DatastoreOptions options =
                DatastoreOptions.newBuilder()
                                .setProjectId(withCustomConverter.getValue())
                                .build();
        NamespaceToTenantIdConverter converter = mock(NamespaceToTenantIdConverter.class);
        DatastoreStorageFactory factory =
                DatastoreStorageFactory.newBuilder()
                                       .setMultitenant(true)
                                       .setDatastore(options.getService())
                                       .setNamespaceToTenantIdConverter(converter)
                                       .build();
        assertNotNull(factory);

        Optional<NamespaceToTenantIdConverter> restoredConverter =
                TenantConverterRegistry.getNamespaceConverter(withCustomConverter);
        assertTrue(restoredConverter.isPresent());
        assertSame(converter, restoredConverter.get());

        Optional<NamespaceToTenantIdConverter> absentConverter =
                TenantConverterRegistry.getNamespaceConverter(withDefaultConverter);
        assertFalse(absentConverter.isPresent());
    }

    private static Datastore mockDatastore() {
        return TestDatastoreFactory.getLocalDatastore();
    }

    private static <T> EntityColumn mockColumn(Class<T> type) {
        EntityColumn mock = mock(EntityColumn.class);
        when(mock.getType()).thenReturn(type);
        when(mock.getPersistedType()).thenReturn(type);
        return mock;
    }

    private static class MockByteColumnType extends SimpleDatastoreColumnType<Byte> {
        @Override
        public void setColumnValue(BaseEntity.Builder storageRecord, Byte value, String columnIdentifier) {
            // NOP
        }
        @Override
        public Value<?> toValue(Byte data) {
            // NOP
            return nullRef();
        }
    }
}
