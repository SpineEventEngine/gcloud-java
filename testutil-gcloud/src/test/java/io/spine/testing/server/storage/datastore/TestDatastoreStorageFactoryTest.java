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

package io.spine.testing.server.storage.datastore;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.testing.NullPointerTester;
import io.spine.server.storage.datastore.DatastoreWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.testing.server.storage.datastore.given.AnEntity.withKeyCreatedBy;

@DisplayName("`TestDatastoreStorageFactory` should")
class TestDatastoreStorageFactoryTest {

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() {
        new NullPointerTester()
                .testAllPublicStaticMethods(TestDatastoreStorageFactory.class);
    }

    @Test
    @DisplayName("wrap the specified Datastore with a `TestDatastoreWrapper`")
    void wrapDatastore() {
        TestDatastoreStorageFactory factory = TestDatastoreStorageFactory.local();
        DatastoreWrapper wrapper = factory.createDatastoreWrapper(false);
        assertThat(wrapper).isInstanceOf(TestDatastoreWrapper.class);
    }

    @Test
    @DisplayName("clear all data in the Datastore")
    void clearDatastore() {
        // Initialize the factory.
        TestDatastoreStorageFactory factory = TestDatastoreStorageFactory.local();
        DatastoreWrapper wrapper = factory.createDatastoreWrapper(false);

        // Create an entity.
        Entity entity = withKeyCreatedBy(wrapper);
        Key key = entity.getKey();
        wrapper.createOrUpdate(entity);

        // Make sure the entity is read from the Datastore by key.
        Entity entityReadBeforeClear = wrapper.read(key);
        assertThat(entityReadBeforeClear).isNotNull();

        // Clear the Datastore.
        factory.clear();

        // Make sure entity is no longer present.
        Entity entityReadAfterClear = wrapper.read(key);
        assertThat(entityReadAfterClear).isNull();
    }
}
