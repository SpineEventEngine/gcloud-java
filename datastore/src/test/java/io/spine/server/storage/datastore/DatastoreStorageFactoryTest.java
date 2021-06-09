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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Value;
import com.google.common.testing.NullPointerTester;
import io.spine.base.Identifier;
import io.spine.core.TenantId;
import io.spine.server.ContextSpec;
import io.spine.server.storage.ColumnMapping;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.given.DatastoreStorageFactoryTestEnv.DifferentTestEntity;
import io.spine.server.storage.datastore.given.DatastoreStorageFactoryTestEnv.TestEntity;
import io.spine.server.storage.datastore.given.TestEnvironment;
import io.spine.server.storage.datastore.record.DsRecordStorage;
import io.spine.server.storage.datastore.record.RecordId;
import io.spine.test.storage.StgProject;
import io.spine.test.storage.StgProjectId;
import io.spine.type.TypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.ContextSpec.multitenant;
import static io.spine.server.storage.datastore.given.DatastoreStorageFactoryTestEnv.factoryFor;
import static io.spine.server.storage.datastore.given.TestEnvironment.singleTenantSpec;
import static io.spine.server.storage.datastore.given.TestRecordSpec.stgProjectSpec;
import static io.spine.server.tenant.TenantAwareRunner.with;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.testing.server.storage.datastore.TestDatastores.defaultLocalProjectId;
import static io.spine.testing.server.storage.datastore.TestDatastores.local;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("`DatastoreStorageFactory` should")
final class DatastoreStorageFactoryTest {

    private static final DatastoreOptions DUMMY_OPTIONS =
            DatastoreOptions.newBuilder()
                            .setProjectId("dummy-dataset")
                            .build();

    private static final Datastore datastore = DUMMY_OPTIONS.getService();

    private static final DatastoreStorageFactory factory =
            factoryFor(datastore);

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void testNulls() {
        new NullPointerTester().setDefault(ContextSpec.class, singleTenantSpec())
                               .setDefault(RecordSpec.class, stgProjectSpec())
                               .testAllPublicInstanceMethods(factory);
    }

    @Test
    @DisplayName("create multitenant storages")
    void testCreateMultitenant() {
        StorageFactory factory = factoryFor(datastore);
        RecordStorage<?, ?> storage =
                factory.createRecordStorage(TestEnvironment.multiTenantSpec(), TestEntity.spec());
        assertTrue(storage.isMultitenant());
        storage.close();
    }

    @Test
    @DisplayName("create separate record storage per state type")
    void testDependsOnStateType() {
        ContextSpec contextSpec = singleTenantSpec();
        DsRecordStorage<?, ?> storage = (DsRecordStorage<?, ?>)
                factory.createRecordStorage(contextSpec, TestEntity.spec());
        assertNotNull(storage);
        DsRecordStorage<?, ?> differentStorage = (DsRecordStorage<?, ?>)
                factory.createRecordStorage(contextSpec, DifferentTestEntity.spec());
        assertNotNull(differentStorage);
        assertNotEquals(storage.kind(), differentStorage.kind());
    }

    @Test
    @DisplayName("have default column mapping")
    void testDefaultColumnMapping() {
        DatastoreStorageFactory factory = factoryFor(datastore);
        ColumnMapping<Value<?>> mapping = factory.columnMapping();
        assertNotNull(mapping);
    }

    @Test
    @DisplayName("do nothing on close")
    void testClose() {
        DatastoreStorageFactory factory = factoryFor(datastore);
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
        DatastoreStorageFactory factory = factoryFor(datastore);
        RecordStorage<StgProjectId, StgProject> storage =
                factory.createRecordStorage(spec, stgProjectSpec());
        Key key = writeForTenant(storage, tenant)
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
        DatastoreStorageFactory factory = factoryFor(datastore);
        RecordStorage<StgProjectId, StgProject> storage =
                factory.createRecordStorage(spec, stgProjectSpec());
        Key key = writeForTenant(storage, tenant)
                .setNamespace('V' + tenant.getValue())
                .build();
        Entity entity = datastore.get(key);
        assertThat(entity).isNotNull();
    }

    private static Key.Builder writeForTenant(RecordStorage<StgProjectId, StgProject> storage,
                                              TenantId tenant) {
        StgProjectId id = StgProjectId.newBuilder()
                                      .setId(Identifier.newUuid())
                                      .vBuild();
        StgProject project = StgProject.newBuilder()
                                       .setId(id)
                                       .setName("Sample storage project")
                                       .vBuild();
        with(tenant).run(
                () -> storage.write(id, project)
        );
        RecordId recordId = RecordId.ofEntityId(id);
        return Key.newBuilder(defaultLocalProjectId().value(),
                              TypeName.of(project)
                                      .value(),
                              recordId.value());
    }
}
