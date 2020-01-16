/*
 * Copyright 2020, TeamDev. All rights reserved.
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
import com.google.cloud.datastore.Value;
import com.google.common.testing.NullPointerTester;
import io.spine.server.ContextSpec;
import io.spine.server.entity.storage.ColumnMapping;
import io.spine.server.storage.datastore.given.TestColumnMapping;
import io.spine.testing.server.storage.datastore.TestDatastores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.ContextSpec.singleTenant;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static io.spine.testing.server.storage.datastore.TestDatastores.defaultLocalProjectId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
                            datastore())
                .setDefault(ColumnMapping.class, new DsColumnMapping())
                .testInstanceMethods(DatastoreStorageFactory.newBuilder(),
                                     NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    @DisplayName("construct factories with default column mapping")
    void testDefaultColumnMapping() {
        DatastoreStorageFactory factory = DatastoreStorageFactory
                .newBuilder()
                .setDatastore(datastore())
                .build();
        ColumnMapping<Value<?>> mapping = factory.columnMapping();
        assertNotNull(mapping);
    }

    @Test
    @DisplayName("construct factories with custom column mapping")
    void testCustomColumnMapping() {
        ColumnMapping<Value<?>> mapping = new TestColumnMapping();
        DatastoreStorageFactory factory = DatastoreStorageFactory
                .newBuilder()
                .setDatastore(datastore())
                .setColumnMapping(mapping)
                .build();
        ColumnMapping<Value<?>> mappingUsedByFactory = factory.columnMapping();
        assertNotNull(mappingUsedByFactory);

        String someString = "some-test-string";
        Value<?> value = mappingUsedByFactory.of(String.class)
                                             .applyTo(someString);
        assertThat(value).isEqualTo(TestColumnMapping.STRING_MAPPING_RESULT);
    }

    @Nested
    class Namespaces {

        private DatastoreOptions.Builder builder;

        @BeforeEach
        void setUp() {
            builder = DatastoreOptions
                    .newBuilder()
                    .setProjectId(defaultLocalProjectId().getValue());
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
                                            .datastore()
                                            .getOptions()
                                            .getNamespace();
            assertEquals(namespace, actualNamespace);
        }
    }

    @Test
    @DisplayName("fail to construct without Datastore")
    void testRequireDatastore() {
        assertThrows(NullPointerException.class, DatastoreStorageFactory.newBuilder()::build);
    }

    private static Datastore datastore() {
        return TestDatastores.local();
    }
}
