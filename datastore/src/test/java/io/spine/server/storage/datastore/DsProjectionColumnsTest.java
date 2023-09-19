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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.spine.base.Time;
import io.spine.core.Version;
import io.spine.core.Versions;
import io.spine.protobuf.AnyPacker;
import io.spine.server.ContextSpec;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.given.Given;
import io.spine.server.entity.storage.ColumnTypeMapping;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.datastore.tenant.given.CollegeProjection;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.datastore.RecordId.ofEntityId;
import static io.spine.server.storage.datastore.given.TestEnvironment.singleTenantSpec;
import static io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory.local;

@DisplayName("When dealing with `Projection` columns, `DsProjectionStorage` should")
final class DsProjectionColumnsTest {

    private static final TestDatastoreStorageFactory datastoreFactory = local(new CustomMapping());

    @Test
    @DisplayName("store Proto's `Timestamp` fields with the respect " +
            "of Datastore's support of `Timestamps`")
    void storeTimestamps() {
        ContextSpec spec = singleTenantSpec();
        Class<CollegeProjection> projectionCls = CollegeProjection.class;
        ProjectionStorage<CollegeId> storage =
                datastoreFactory.createProjectionStorage(spec, projectionCls);
        CollegeId id = CollegeId.newBuilder()
                                .setValue(newUuid())
                                .vBuild();
        DatastoreWrapper datastore = datastoreFactory.createDatastoreWrapper(false);

        Kind collegeKind = Kind.of(TypeUrl.from(College.getDescriptor()));
        Key key = datastore.keyFor(collegeKind, ofEntityId(id));

        Timestamp admissionDeadline = Timestamps.add(Time.currentTime(), Durations.fromDays(100));
        College college =
                College.newBuilder()
                       .setId(id)
                       .setName("Alma")
                       .setStudentCount(42)
                       .setAdmissionDeadline(admissionDeadline)
                       .setPassingGrade(4.2)
                       .setStateSponsored(false)
                       .setCreated(Time.currentTime())
                       .addAllSubjects(
                               ImmutableList.of("English Literature", "Engineering", "Psychology"))
                       .vBuild();
        Version version = Versions.newVersion(42, Time.currentTime());
        EntityRecord record = EntityRecord
                .newBuilder()
                .setState(AnyPacker.pack(college))
                .setVersion(version)
                .vBuild();
        CollegeProjection projection =
                Given.projectionOfClass(CollegeProjection.class)
                     .withId(id)
                     .withState(college)
                     .withVersion(version.getNumber())
                     .build();
        EntityRecordWithColumns recordWithCols =
                EntityRecordWithColumns.create(record, projection, storage);
        storage.write(id, recordWithCols);
        Entity response = datastore.read(key);

        com.google.cloud.Timestamp storedDeadline = readAdmissionDeadline(response);
        assertThat(storedDeadline).isNotNull();

        // ------

        College collegeNoAdmission = college.toBuilder()
                                  .clearAdmissionDeadline()
                                  .vBuild();
        CollegeProjection projectionNoAdmission =
                Given.projectionOfClass(CollegeProjection.class)
                     .withId(id)
                     .withState(collegeNoAdmission)
                     .withVersion(version.getNumber())
                     .build();
        EntityRecord recordNoAdmission = EntityRecord
                .newBuilder()
                .setState(AnyPacker.pack(collegeNoAdmission))
                .setVersion(version)
                .vBuild();
        EntityRecordWithColumns recordWithColsNoAdmission =
                EntityRecordWithColumns.create(recordNoAdmission, projectionNoAdmission, storage);
        storage.write(id, recordWithColsNoAdmission);

        Entity responseWithNoAdmission = datastore.read(key);
        com.google.cloud.Timestamp presumablyEmptyDeadline =
                readAdmissionDeadline(responseWithNoAdmission);
        assertThat(presumablyEmptyDeadline)
                .isNull();
    }

    private static com.google.cloud.Timestamp readAdmissionDeadline(Entity response) {
        com.google.cloud.Timestamp storedTimestamp = response.getTimestamp(
                College.Column.admissionDeadline()
                              .name()
                              .value());
        return storedTimestamp;
    }


    private static final class CustomMapping extends DsColumnMapping {

        @Override
        protected void setupCustomMapping(
                ImmutableMap.Builder<Class<?>, ColumnTypeMapping<?, ? extends Value<?>>> builder) {
            super.setupCustomMapping(builder);
            builder.put(Timestamp.class, ofNullableTimestamp());
        }

        @SuppressWarnings("UnnecessaryLambda")
        private static ColumnTypeMapping<Timestamp, Value<?>> ofNullableTimestamp() {
            return timestamp -> {
                if (timestamp.equals(Timestamp.getDefaultInstance())) {
                    return NullValue.of();
                }
                return TimestampValue.of(
                        ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos())
                );
            };
        }
    }
}
