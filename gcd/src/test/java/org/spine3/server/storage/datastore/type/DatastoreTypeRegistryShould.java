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

package org.spine3.server.storage.datastore.type;

import com.google.cloud.datastore.Entity;
import com.google.protobuf.Timestamp;
import org.junit.Test;
import org.spine3.base.Version;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.ColumnTypeRegistry;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class DatastoreTypeRegistryShould {

    @Test
    public void have_default_column_types() {
        final ColumnTypeRegistry<DatastoreColumnType> registry = DatastoreTypeRegistry.defaultInstance();
        final DatastoreColumnType<?, ?> stringType = registry.get(mockColumn(String.class));
        assertNotNull(stringType);
        final DatastoreColumnType<?, ?> intType = registry.get(mockColumn(int.class));
        assertNotNull(intType);
        final DatastoreColumnType<?, ?> booleanType = registry.get(mockColumn(boolean.class));
        assertNotNull(booleanType);
        final DatastoreColumnType<?, ?> messageType = registry.get(mockColumn(String.class));
        assertNotNull(messageType);
        final DatastoreColumnType<?, ?> timestampType = registry.get(mockColumn(Timestamp.class));
        assertNotNull(timestampType);
        final DatastoreColumnType<?, ?> versionType = registry.get(mockColumn(Version.class));
        assertNotNull(versionType);
    }

    @Test
    public void allow_to_customize_types() {
        final ColumnTypeRegistry<DatastoreColumnType> registry =
                DatastoreTypeRegistry.predefinedValuesAnd()
                                     .put(byte.class, new ByteColumnType())
                                     .build();
        final DatastoreColumnType byteColumnType = registry.get(mockColumn(Byte.class));
        assertNotNull(byteColumnType);
        assertThat(byteColumnType, instanceOf(ByteColumnType.class));
    }

    @Test
    public void allow_to_override_types() {
        final ColumnTypeRegistry<DatastoreColumnType> registry =
                DatastoreTypeRegistry.predefinedValuesAnd()
                                     .put(String.class, new CustomStringType())
                                     .build();
        final DatastoreColumnType byteColumnType = registry.get(mockColumn(String.class));
        assertNotNull(byteColumnType);
        assertThat(byteColumnType, instanceOf(CustomStringType.class));
    }

    private static Column<?> mockColumn(Class type) {
        final Column<?> column = mock(Column.class);
        when(column.getType()).thenReturn(type);
        return column;
    }

    private static class ByteColumnType extends SimpleDatastoreColumnType<Byte> {
        @Override
        public void setColumnValue(Entity.Builder storageRecord, Byte value, String columnIdentifier) {
            storageRecord.set(columnIdentifier, value);
        }
    }

    private static class CustomStringType implements DatastoreColumnType<String, Integer> {

        @Override
        public Integer convertColumnValue(String fieldValue) {
            return fieldValue.length();
        }

        @Override
        public void setColumnValue(Entity.Builder storageRecord, Integer value, String columnIdentifier) {
            storageRecord.set(columnIdentifier, value);
        }
    }
}
