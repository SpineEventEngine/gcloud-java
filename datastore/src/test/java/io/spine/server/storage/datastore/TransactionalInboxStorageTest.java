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

import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxStorageTest;
import io.spine.server.storage.datastore.delivery.InboxStorageLayout;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import io.spine.testing.server.storage.datastore.TestDatastores;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

/**
 * Tests the {@code InboxStorage} with the transactions/tree-layout enabled.
 *
 * @see InboxStorageLayout for more details on ancestor-child relations used for this test suite
 */
final class TransactionalInboxStorageTest extends InboxStorageTest {

    private static final TestDatastoreStorageFactory datastoreFactory = withTransactionalInbox();

    @BeforeAll
    static void setUpClass() {
        datastoreFactory.setUp();
        ServerEnvironment
                .when(Tests.class)
                .useStorageFactory((env) -> datastoreFactory);
    }

    private static TestDatastoreStorageFactory withTransactionalInbox() {
        var builder =
                DatastoreStorageFactory
                        .newBuilderWithDefaults(TestDatastores.local())
                        .enableTransactions(InboxMessage.class)
                        .organizeRecords(InboxMessage.class, new InboxStorageLayout());
        var factory = TestDatastoreStorageFactory.basedOn(builder);
        return factory;
    }

    @AfterEach
    void tearDownTest() {
        datastoreFactory.clear();
    }

    @AfterAll
    static void tearDownClass() {
        datastoreFactory.tearDown();
    }
}
