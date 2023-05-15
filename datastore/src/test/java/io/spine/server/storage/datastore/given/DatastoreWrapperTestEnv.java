/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore.given;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import io.spine.core.TenantId;
import io.spine.server.storage.datastore.DatastoreWrapper;
import io.spine.server.storage.datastore.Kind;
import io.spine.server.tenant.TenantAwareOperation;
import io.spine.testing.server.storage.datastore.TestDatastores;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A test environment for {@link io.spine.server.storage.datastore.DatastoreWrapperTest}.
 */
public class DatastoreWrapperTestEnv {

    public static final Kind NAMESPACE_HOLDER_KIND = Kind.of("spine.test.NAMESPACE_HOLDER_KIND");
    public static final Kind GENERIC_ENTITY_KIND = Kind.of("my.entity");

    private static final String SERVICE_ACCOUNT_RESOURCE_PATH = "spine-dev.json";

    /**
     * Prevents instantiation of this test environment.
     */
    private DatastoreWrapperTestEnv() {
    }

    public static void checkTenantIdInKey(String id, TenantId tenantId, DatastoreWrapper wrapper) {
        new TenantAwareOperation(tenantId) {
            @Override
            public void run() {
                Key key = wrapper.keyFactory(GENERIC_ENTITY_KIND)
                                 .newKey(42L);
                assertEquals(id, key.getNamespace());
            }
        }.execute();
    }

    public static void ensureNamespace(String namespaceValue, Datastore datastore) {
        KeyFactory keyFactory = datastore.newKeyFactory()
                                         .setNamespace(namespaceValue)
                                         .setKind(NAMESPACE_HOLDER_KIND.value());
        Entity entity = Entity.newBuilder(keyFactory.newKey(42L))
                              .build();
        datastore.put(entity);
    }

    public static Datastore localDatastore() {
        return TestDatastores.local();
    }

    public static Datastore remoteDatastore() {
        return TestDatastores.remote(SERVICE_ACCOUNT_RESOURCE_PATH);
    }
}
