/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.spine3.base.Identifiers;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorageShould;
import org.spine3.server.storage.RecordStorage;
import org.spine3.test.aggregate.Project;

public class DsEntityStorageShould extends EntityStorageShould<String> {

    private static final TestDatastoreStorageFactory DATASTORE_FACTORY = TestDatastoreStorageFactory.getDefaultInstance();

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


    @Override
    protected RecordStorage<String> getStorage() {
        return DATASTORE_FACTORY.createRecordStorage(TestEntity.class);
    }

    @Override
    protected String newId() {
        return Identifiers.newUuid();
    }

    @Override
    protected <I> RecordStorage<I> getStorage(Class<? extends Entity<I, ?>> entityClass) {
        return DATASTORE_FACTORY.createRecordStorage(entityClass);
    }

    private static class TestEntity extends Entity<String, Project> {

        /**
         * Creates a new instance.
         *
         * @param id the ID for the new instance
         * @throws IllegalArgumentException if the ID is not of one of the supported types for identifiers
         */
        private TestEntity(String id) {
            super(id);
        }
    }
}
