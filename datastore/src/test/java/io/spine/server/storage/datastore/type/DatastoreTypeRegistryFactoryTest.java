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

package io.spine.server.storage.datastore.type;

import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Value;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.datastore.given.Columns;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.datastore.given.Columns.booleanColumn;
import static io.spine.server.storage.datastore.given.Columns.byteColumn;
import static io.spine.server.storage.datastore.given.Columns.doubleColumn;
import static io.spine.server.storage.datastore.given.Columns.floatColumn;
import static io.spine.server.storage.datastore.given.Columns.intColumn;
import static io.spine.server.storage.datastore.given.Columns.longColumn;
import static io.spine.server.storage.datastore.given.Columns.messageColumn;
import static io.spine.server.storage.datastore.given.Columns.stringColumn;
import static io.spine.server.storage.datastore.given.Columns.timestampColumn;
import static io.spine.server.storage.datastore.given.Columns.versionColumn;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("DatastoreTypeRegistryFactory should")
class DatastoreTypeRegistryFactoryTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void testPrivateCtor() {
        assertHasPrivateParameterlessCtor(DatastoreTypeRegistryFactory.class);
    }

    @Test
    @DisplayName("have default column types")
    void testDefaults() {
        ColumnTypeRegistry<? extends DatastoreColumnType> registry =
                DatastoreTypeRegistryFactory.defaultInstance();
        DatastoreColumnType<?, ?> stringType = registry.get(stringColumn());
        assertNotNull(stringType);
        DatastoreColumnType<?, ?> intType = registry.get(intColumn());
        assertNotNull(intType);
        DatastoreColumnType<?, ?> longType = registry.get(longColumn());
        assertNotNull(longType);
        DatastoreColumnType<?, ?> floatType = registry.get(floatColumn());
        assertNotNull(floatType);
        DatastoreColumnType<?, ?> doubleType = registry.get(doubleColumn());
        assertNotNull(doubleType);
        DatastoreColumnType<?, ?> booleanType = registry.get(booleanColumn());
        assertNotNull(booleanType);
        DatastoreColumnType<?, ?> messageType = registry.get(messageColumn());
        assertNotNull(messageType);
        DatastoreColumnType<?, ?> timestampType = registry.get(timestampColumn());
        assertNotNull(timestampType);
        DatastoreColumnType<?, ?> versionType = registry.get(versionColumn());
        assertNotNull(versionType);
    }

    @Test
    @DisplayName("allow to customize types")
    void testCustomize() {
        ColumnTypeRegistry<? extends DatastoreColumnType> registry =
                DatastoreTypeRegistryFactory.predefinedValuesAnd()
                                            .put(byte.class, new Columns.ByteColumnType())
                                            .build();
        DatastoreColumnType byteColumnType = registry.get(byteColumn());
        assertNotNull(byteColumnType);
        assertThat(byteColumnType, instanceOf(Columns.ByteColumnType.class));
    }

    @Test
    @DisplayName("allow to override types")
    void testOverride() {
        ColumnTypeRegistry<? extends DatastoreColumnType> registry =
                DatastoreTypeRegistryFactory.predefinedValuesAnd()
                                            .put(String.class, new CustomStringType())
                                            .build();
        DatastoreColumnType columnType = registry.get(stringColumn());
        assertNotNull(columnType);
        assertThat(columnType, instanceOf(CustomStringType.class));
    }

    private static class CustomStringType extends AbstractDatastoreColumnType<String, Integer> {

        @Override
        public Integer convertColumnValue(String fieldValue) {
            return fieldValue.length();
        }

        @Override
        public Value<?> toValue(Integer data) {
            return LongValue.of(data);
        }
    }
}
