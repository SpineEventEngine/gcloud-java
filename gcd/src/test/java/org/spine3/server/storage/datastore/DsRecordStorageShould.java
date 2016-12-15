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

import com.google.protobuf.Message;
import org.junit.After;
import org.junit.Before;
import org.spine3.base.Identifiers;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AbstractStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.RecordStorageShould;
import org.spine3.test.storage.Project;
import org.spine3.test.storage.ProjectId;
import org.spine3.test.storage.Task;

/**
 * @author Dmytro Dashenkov
 */
public class DsRecordStorageShould extends RecordStorageShould<ProjectId> {

    private static final TestDatastoreStorageFactory LOCAL_DATASTORE_STORAGE_FACTORY
            = TestDatastoreStorageFactory.getDefaultInstance();

    @Before
    public void setUp() throws Exception {
        LOCAL_DATASTORE_STORAGE_FACTORY.setUp();
    }

    @After
    public void tearDown() throws Exception {
        LOCAL_DATASTORE_STORAGE_FACTORY.tearDown();
    }

    @Override
    protected Message newState(ProjectId projectId) {
        final Project project = Project.newBuilder()
                                       .setId(projectId)
                                       .setName("Some test name")
                                       .addTask(Task.getDefaultInstance())
                                       .setStatus(Project.Status.CREATED)
                                       .build();
        return project;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <S extends AbstractStorage<ProjectId, EntityStorageRecord>> S getStorage() {
        return (S) LOCAL_DATASTORE_STORAGE_FACTORY.createRecordStorage(TestAggregate.class);
    }

    @Override
    protected ProjectId newId() {
        final ProjectId projectId = ProjectId.newBuilder()
                .setId(Identifiers.newUuid())
                .build();
        return projectId;
    }

    private static class TestAggregate extends Aggregate<ProjectId, Project, Project.Builder> {

        /**
         * Creates a new aggregate instance.
         *
         * @param id the ID for the new aggregate
         * @throws IllegalArgumentException if the ID is not of one of the supported types
         */
        private TestAggregate(ProjectId id) {
            super(id);
        }
    }
}