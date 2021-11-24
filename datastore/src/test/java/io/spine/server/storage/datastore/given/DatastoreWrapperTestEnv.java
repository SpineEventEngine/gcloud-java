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

package io.spine.server.storage.datastore.given;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import io.spine.server.storage.datastore.Kind;
import io.spine.testing.server.storage.datastore.TestDatastores;

/**
 * A test environment for {@link io.spine.server.storage.datastore.DatastoreWrapperTest}.
 */
public final class DatastoreWrapperTestEnv {

    public static final Kind NAMESPACE_HOLDER_KIND = Kind.of("spine.test.NAMESPACE_HOLDER_KIND");

    private static final String SERVICE_ACCOUNT_RESOURCE_PATH = "spine-dev.json";

    /**
     * Prevents instantiation of this test environment.
     */
    private DatastoreWrapperTestEnv() {
    }

    /**
     * Forces Datastore to create a namespace with the given name.
     */
    public static void ensureNamespace(String namespaceValue, Datastore datastore) {
        var keyFactory = datastore.newKeyFactory()
                                  .setNamespace(namespaceValue)
                                  .setKind(NAMESPACE_HOLDER_KIND.value());
        var entity = Entity
                .newBuilder(keyFactory.newKey(42L))
                .build();
        datastore.put(entity);
    }

    /**
     * Returns the {@code Datastore} instance connected to the Datastore emulator
     * running {@linkplain TestDatastores#local() locally}.
     */
    public static Datastore localDatastore() {
        return TestDatastores.local();
    }

    /**
     * Returns the {@code Datastore} instance connected to the Datastore instance
     * running remotely.
     *
     * <p>The connection to a remote Datastore instance is performed via the service
     * account defined by the {@code spine-dev.json} credential file.
     */
    public static Datastore remoteDatastore() {
        return TestDatastores.remote(SERVICE_ACCOUNT_RESOURCE_PATH);
    }
}
