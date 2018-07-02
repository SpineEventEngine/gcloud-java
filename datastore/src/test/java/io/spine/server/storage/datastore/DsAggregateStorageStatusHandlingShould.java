/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageVisibilityHandlingTest;
import io.spine.test.aggregate.ProjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dmytro Dashenkov.
 */
public class DsAggregateStorageStatusHandlingShould
        extends AggregateStorageVisibilityHandlingTest {

    private static final TestDatastoreStorageFactory datastoreFactory;

    // Guarantees any stacktrace to be informative
    static {
        try {
            datastoreFactory = TestDatastoreStorageFactory.getDefaultInstance();
        } catch (Throwable e) {
            log().error("Could not initialize test datastore factory {}", e);
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void setUpClass() {
        datastoreFactory.setUp();
    }

    @AfterEach
    public void tearDownTest() {
        datastoreFactory.clear();
    }

    @AfterAll
    public static void tearDownClass() {
        datastoreFactory.tearDown();
    }

    @Override
    protected AggregateStorage<ProjectId> getAggregateStorage(
            Class<? extends Aggregate<ProjectId, ?, ?>> aggregateClass) {
        return datastoreFactory.createAggregateStorage(aggregateClass);
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(
                DsAggregateStorageStatusHandlingShould.class);
    }

}
