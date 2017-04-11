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

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Datastore;
import com.google.common.testing.NullPointerTester;
import org.junit.Test;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.ColumnType;
import org.spine3.server.entity.storage.ColumnTypeRegistry;
import org.spine3.server.storage.StorageFactory;
import org.spine3.server.storage.datastore.type.DatastoreTypeRegistry;
import org.spine3.server.storage.datastore.type.SimpleDatastoreColumnType;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.spine3.test.Tests.assertHasPrivateParameterlessCtor;

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
                .setDefault(ColumnTypeRegistry.class, DatastoreTypeRegistry.defaultInstance())
                .testInstanceMethods(DatastoreStorageFactory.newBuilder(), NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    public void construct_factories_with_default_type_registry() {
        final StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                              .setDatastore(mockDatastore())
                                                              .build();
        final ColumnTypeRegistry registry = factory.getTypeRegistry();
        assertNotNull(registry);
    }

    @Test
    public void construct_single_tenant_factories_by_default() {
        final StorageFactory factory = DatastoreStorageFactory.newBuilder()
                                                              .setDatastore(mockDatastore())
                                                              .build();
        assertFalse(factory.isMultitenant());
    }

    @Test
    public void construct_factories_with_extended_type_registry() {
        final StorageFactory factory =
                DatastoreStorageFactory.newBuilder()
                                       .setDatastore(mockDatastore())
                                       .setTypeRegistry(DatastoreTypeRegistry.predefinedValuesAnd()
                                                                             .put(Byte.class, new MockByteColumnType())
                                                                             .build())
                                       .build();
        final ColumnTypeRegistry<?> registry = factory.getTypeRegistry();
        assertNotNull(registry);
        final ColumnType type = registry.get(mockColumn(Byte.class));
        assertNotNull(type);
    }

    private static Datastore mockDatastore() {
        return mock(Datastore.class);
    }

    private static <T> Column<T> mockColumn(Class<T> type) {
        @SuppressWarnings("unchecked")
        final Column<T> mock = mock(Column.class);
        when(mock.getType()).thenReturn(type);
        return mock;
    }

    private static class MockByteColumnType extends SimpleDatastoreColumnType<Byte> {
        @Override
        public void setColumnValue(BaseEntity.Builder storageRecord, Byte value, String columnIdentifier) {
            // NOP
        }
    }
}
