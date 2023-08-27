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

package io.spine.server.storage.datastore.delivery;

import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.delivery.CatchUpTest;
import io.spine.testing.SlowTest;
import io.spine.testing.logging.mute.MuteLogging;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests on {@link io.spine.server.delivery.CatchUp CatchUp} functionality.
 *
 * <p>The tests are extremely slow, so several long tests from {@link CatchUpTest} are disabled.
 */
@SlowTest
@DisplayName("Datastore-backed `CatchUp` should ")
@MuteLogging
class DsCatchUpSmokeTest extends CatchUpTest {

    private TestDatastoreStorageFactory factory;

    @BeforeEach
    void prepareStorageFactory() {
        factory = TestDatastoreStorageFactory.local();
        ServerEnvironment.when(Tests.class)
                         .use(factory);
    }

    @AfterEach
    void tearDownStorageFactory() {
        if (factory != null) {
            factory.tearDown();
        }
    }


    @Test
    @Disabled
    @Override
    public void withNanosByIds() throws InterruptedException {
        super.withNanosByIds();
    }

    @Test
    @Disabled
    @Override
    public void withMillisByIds() throws InterruptedException {
        super.withMillisByIds();
    }

    @Test
    @Disabled
    @Override
    public void withMillisAllInOrder() throws InterruptedException {
        super.withMillisAllInOrder();
    }
}
