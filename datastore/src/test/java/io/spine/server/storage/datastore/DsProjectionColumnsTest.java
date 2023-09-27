/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Key;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;
import io.spine.server.ContextSpec;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.CustomMapping;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.datastore.RecordId.ofEntityId;
import static io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.COLLEGE_CLS;
import static io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.COLLEGE_KIND;
import static io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.clearAdmission;
import static io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.futureFromNow;
import static io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.newCollege;
import static io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.newId;
import static io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.someVersion;
import static io.spine.server.storage.datastore.given.DsProjectionColumnsTestEnv.writeAndReadDeadline;
import static io.spine.server.storage.datastore.given.TestEnvironment.singleTenantSpec;
import static io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory.local;

@DisplayName("When dealing with `Projection` columns, `DsProjectionStorage` should")
final class DsProjectionColumnsTest {

    private static final TestDatastoreStorageFactory datastoreFactory = local(new CustomMapping());

    @Test
    @DisplayName("allow clearing the column values " +
            "if the column mapping used returns Datastore-specific `null`" +
            "for their values")
    void clearTimestampColumns() {
        ContextSpec spec = singleTenantSpec();
        ProjectionStorage<CollegeId> storage =
                datastoreFactory.createProjectionStorage(spec, COLLEGE_CLS);
        DatastoreWrapper datastore = datastoreFactory.createDatastoreWrapper(false);

        CollegeId id = newId();
        Key key = datastore.keyFor(COLLEGE_KIND, ofEntityId(id));
        Version version = someVersion();

        Timestamp admissionDeadline = futureFromNow();
        College college = newCollege(id, admissionDeadline);

        com.google.cloud.Timestamp storedDeadline =
                writeAndReadDeadline(college, version, storage, datastore, key);
        assertThat(storedDeadline).isNotNull();

        College collegeNoAdmission = clearAdmission(college);
        com.google.cloud.Timestamp presumablyEmptyDeadline =
                writeAndReadDeadline(collegeNoAdmission, version, storage, datastore, key);
        assertThat(presumablyEmptyDeadline)
                .isNull();
    }
}
