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
import org.spine3.base.Identifiers;
import org.spine3.server.projection.Projection;
import org.spine3.server.storage.ProjectionStorage;
import org.spine3.server.storage.ProjectionStorageShould;
import org.spine3.test.projection.Project;

/**
 * @author Mikhail Mikhaylov
 */
@SuppressWarnings("RefusedBequest") // Overrides several methods as NoOps
public class DsProjectionStorageShould extends ProjectionStorageShould<String> {
    private static final TestDatastoreStorageFactory DATASTORE_FACTORY = TestDatastoreStorageFactory.getDefaultInstance();

    @After
    public void tearDownTest() {
        DATASTORE_FACTORY.clear();
    }

    @Override
    protected ProjectionStorage<String> getStorage() {
        return DATASTORE_FACTORY.createProjectionStorage(TestProjection.class);
    }

    @Override
    protected String newId() {
        return Identifiers.newUuid();
    }

    private static class TestProjection extends Projection<String, Project> {
        private TestProjection(String id) {
            super(id);
        }
    }
}
