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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.base.Stringifier;
import org.spine3.base.StringifierRegistry;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.aggregate.AggregateStorageShould;
import org.spine3.test.aggregate.ProjectId;
import org.spine3.test.storage.Project;

import static org.junit.Assert.assertNotNull;

@SuppressWarnings("InstanceMethodNamingConvention")
public class DsAggregateStorageShould extends AggregateStorageShould {

    private static final TestDatastoreStorageFactory datastoreFactory;

    // Guarantees any stacktrace to be informative
    static {
        try {
            datastoreFactory = TestDatastoreStorageFactory.getDefaultInstance();
        } catch (Throwable e) {
            log().error("Failed to initialize local datastore factory", e);
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setUpAll() {
        StringifierRegistry.getInstance()
                           .register(new Stringifier<ProjectId>() {
                               @Override
                               protected String toString(ProjectId obj) {
                                   return obj.getId();
                               }

                               @Override
                               protected ProjectId fromString(String s) {
                                   return ProjectId.newBuilder()
                                                   .setId(s)
                                                   .build();
                               }
                           }, ProjectId.class);
    }

    @BeforeClass
    public static void setUpClass() {
        datastoreFactory.setUp();
    }

    @After
    public void tearDownTest() {
        datastoreFactory.clear();
    }

    @AfterClass
    public static void tearDownClass() {
        datastoreFactory.tearDown();
    }

    @SuppressWarnings("ConstantConditions")
    // passing null because this parameter isn't used in this implementation
    @Override
    protected AggregateStorage<ProjectId> getStorage() {
        return datastoreFactory.createAggregateStorage(TestAggregate.class);
    }

    @Override
    protected <Id> AggregateStorage<Id> getStorage(
            Class<? extends Aggregate<Id, ? extends Message, ? extends Message.Builder>> aClass) {
        return datastoreFactory.createAggregateStorage(aClass);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override // Override method with the same behavior to change ID value
    public void write_and_read_event_by_Long_id() {
        final AggregateStorage storage = getStorage(TestAggregateWithIdLong.class);
        final long id = 42L;
        this.writeAndReadEventTest(id, storage);
    }

    @Test
    public void provide_access_to_DatastoreWrapper_for_extensibility() {
        final DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        final DatastoreWrapper datastore = storage.getDatastore();
        assertNotNull(datastore);
    }

    @Test
    public void provide_access_to_PropertyStorage_for_extensibility() {
        final DsAggregateStorage<ProjectId> storage = (DsAggregateStorage<ProjectId>) getStorage();
        final DsPropertyStorage propertyStorage = storage.getPropertyStorage();
        assertNotNull(propertyStorage);
    }

    private static class TestAggregateWithIdLong
            extends Aggregate<Long, Project, org.spine3.test.storage.Project.Builder> {
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
