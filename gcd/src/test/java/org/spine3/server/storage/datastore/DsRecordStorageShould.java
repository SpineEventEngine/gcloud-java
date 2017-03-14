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
import org.junit.Before;
import org.junit.Test;
import org.spine3.base.Identifiers;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.RecordStorageShould;
import org.spine3.test.storage.Project;
import org.spine3.test.storage.ProjectId;
import org.spine3.test.storage.Task;
import org.spine3.type.TypeUrl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Dmytro Dashenkov
 */
public class DsRecordStorageShould extends RecordStorageShould<ProjectId, DsRecordStorage<ProjectId>> {

    private static final TestDatastoreStorageFactory datastoreFactory
            = TestDatastoreStorageFactory.getDefaultInstance();

    @Before
    public void setUp() throws Exception {
        datastoreFactory.setUp();
    }

    @After
    public void tearDown() throws Exception {
        datastoreFactory.tearDown();
    }

    @Test
    public void provide_access_to_DatastoreWrapper_for_extensibility() {
        final DsRecordStorage<ProjectId> storage = getStorage();
        final DatastoreWrapper datastore = storage.getDatastore();
        assertNotNull(datastore);
    }

    @Test
    public void provide_access_to_TypeUrl_for_extensibility() {
        final DsRecordStorage<ProjectId> storage = getStorage();
        final TypeUrl typeUrl = storage.getTypeUrl();
        assertNotNull(typeUrl);

        // According to the `TestAggregate` declaration.
        assertEquals(TypeUrl.of(Project.class), typeUrl);
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

    @Override
    protected DsRecordStorage<ProjectId> getStorage() {
        return (DsRecordStorage<ProjectId>) datastoreFactory.createRecordStorage(TestAggregate.class);
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
