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

package io.spine.server.storage.datastore.record;

import io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.CustomMapping;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.datastore.given.TestEnvironment.singleTenantSpec;
import static io.spine.server.storage.datastore.record.RecordId.ofEntityId;
import static io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.COLLEGE_CLS;
import static io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.COLLEGE_KIND;
import static io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.clearAdmission;
import static io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.futureFromNow;
import static io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.newCollege;
import static io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.newId;
import static io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.someVersion;
import static io.spine.server.storage.datastore.record.given.DsEntityColumnsTestEnv.writeAndReadDeadline;
import static io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory.local;

@DisplayName("When dealing with `Entity` columns, `DsEntityRecordStorage` should")
final class DsEntityColumnsTest {

    private static final TestDatastoreStorageFactory datastoreFactory = local(new CustomMapping());

    @Test
    @DisplayName("allow clearing the column values " +
            "if the column mapping used returns Datastore-specific `null` " +
            "for their values")
    void clearTimestampColumns() {
        var spec = singleTenantSpec();
        var storage = datastoreFactory.createEntityRecordStorage(spec, COLLEGE_CLS);
        var datastore = datastoreFactory.newDatastoreWrapper(false);

        var id = newId();
        var key = datastore.keyFor(COLLEGE_KIND, ofEntityId(id));
        var version = someVersion();

        var admissionDeadline = futureFromNow();
        var college = newCollege(id, admissionDeadline);

        var storedDeadline = writeAndReadDeadline(college, version, storage, datastore, key);
        assertThat(storedDeadline).isNotNull();

        var collegeNoAdmission = clearAdmission(college);
        var presumablyEmptyDeadline =
                writeAndReadDeadline(collegeNoAdmission, version, storage, datastore, key);
        assertThat(presumablyEmptyDeadline)
                .isNull();
    }
}
