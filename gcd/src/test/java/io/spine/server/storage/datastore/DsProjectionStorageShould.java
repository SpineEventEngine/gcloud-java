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

package io.spine.server.storage.datastore;

import org.junit.After;
import org.junit.Test;
import io.spine.base.Identifier;
import io.spine.base.Version;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.EntityRecord;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.projection.ProjectionStorageShould;
import io.spine.test.projection.Project;
import io.spine.test.projection.ProjectValidatingBuilder;
import io.spine.testdata.Sample;

import static org.junit.Assert.assertNotNull;
import static io.spine.time.Time.getCurrentTime;

/**
 * @author Mikhail Mikhaylov
 */
public class DsProjectionStorageShould extends ProjectionStorageShould<String> {
    private static final TestDatastoreStorageFactory datastoreFactory =
            TestDatastoreStorageFactory.getDefaultInstance();

    @SuppressWarnings({"MagicNumber", "MethodDoesntCallSuperMethod"})
    @Override
    protected EntityRecord newStorageRecord() {
        return EntityRecord.newBuilder()
                .setState(
                        AnyPacker.pack(Sample.messageOfType(Project.class)))
                .setVersion(Version.newBuilder().setNumber(42).setTimestamp(getCurrentTime()))
                .build();
    }

    @After
    public void tearDownTest() {
        datastoreFactory.clear();
    }

    @Override
    protected ProjectionStorage<String> getStorage() {
        return datastoreFactory.createProjectionStorage(TestProjection.class);
    }

    @Override
    protected String newId() {
        return Identifier.newUuid();
    }

    @Test
    public void provide_access_to_PropertyStorage_for_extensibility() {
        final DsProjectionStorage<String> storage = (DsProjectionStorage<String>) getStorage();
        final DsPropertyStorage propertyStorage = storage.propertyStorage();
        assertNotNull(propertyStorage);
    }

    private static class TestProjection extends Projection<String,
                                                Project,
                                                ProjectValidatingBuilder> {
        private TestProjection(String id) {
            super(id);
        }
    }
}
