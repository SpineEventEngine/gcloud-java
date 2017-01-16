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

package org.spine3.server.storage.datastore;

import com.google.protobuf.Message;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageShould;
import org.spine3.test.storage.Project;
import org.spine3.test.storage.ProjectId;

@SuppressWarnings("InstanceMethodNamingConvention")
public class DsAggregateStorageShould extends AggregateStorageShould {

    private static final TestDatastoreStorageFactory DATASTORE_FACTORY;

    // Guarantees any stacktrace to be informative
    static {
        try {
            DATASTORE_FACTORY = TestDatastoreStorageFactory.getDefaultInstance();
        } catch (Throwable e) {
            log().error("Failed to initialize local datastore factory", e);
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setUpClass() {
        DATASTORE_FACTORY.setUp();
    }

    @After
    public void tearDownTest() {
        DATASTORE_FACTORY.clear();
    }

    @AfterClass
    public static void tearDownClass() {
        DATASTORE_FACTORY.tearDown();
    }

    @SuppressWarnings("ConstantConditions") // passing null because this parameter isn't used in this implementation
    @Override
    protected AggregateStorage<ProjectId> getStorage() {
        return DATASTORE_FACTORY.createAggregateStorage(null);
    }

    @Override
    protected <Id> AggregateStorage<Id> getStorage(Class<? extends Aggregate<Id, ? extends Message, ? extends Message.Builder>> aClass) {
        return DATASTORE_FACTORY.createAggregateStorage(aClass);
    }

    @SuppressWarnings("RefusedBequest")
    @Override // Override method with the same behavior to change ID value
    public void write_and_read_event_by_Long_id() {
        final AggregateStorage storage = getStorage(TestAggregateWithIdLong.class);
        final long id = 42L;
        this.writeAndReadEventTest(id, storage);
    }

    private static class TestAggregateWithIdLong extends Aggregate<Long, Project, org.spine3.test.storage.Project.Builder> {
        private TestAggregateWithIdLong(Long id) {
            super(id);
        }
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(DsAggregateStorageShould.class);
    }
}
