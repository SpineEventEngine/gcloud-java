/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import io.spine.core.Versions;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.projection.ProjectionStorageTest;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.testdata.Sample;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;

import static io.spine.base.Time.currentTime;
import static io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory.local;
import static io.spine.testing.server.storage.datastore.TestEnvironment.singleTenantSpec;

@DisplayName("`DsProjectionStorage` should")
class DsProjectionStorageTest extends ProjectionStorageTest {

    private static final TestDatastoreStorageFactory datastoreFactory = local();

    @Override
    protected Class<? extends TestCounterEntity> getTestEntityClass() {
        return TestCounterEntity.class;
    }

    @Override
    protected EntityRecord newStorageRecord() {
        return EntityRecord
                .newBuilder()
                .setState(AnyPacker.pack(Sample.messageOfType(Project.class)))
                .setVersion(Versions.newVersion(42, currentTime()))
                .vBuild();
    }

    @AfterEach
    void tearDownTest() {
        datastoreFactory.clear();
    }

    @SuppressWarnings("unchecked") // Required for test purposes.
    @Override
    protected ProjectionStorage<ProjectId> newStorage(Class<? extends Entity<?, ?>> cls) {
        Class<? extends Projection<ProjectId, ?, ?>> projectionClass =
                (Class<? extends Projection<ProjectId, ?, ?>>) cls;
        ProjectionStorage<ProjectId> result =
                datastoreFactory.createProjectionStorage(singleTenantSpec(), projectionClass);
        return result;
    }
}
