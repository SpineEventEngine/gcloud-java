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

package io.spine.server.storage.datastore.given;

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
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.given.Given;
import io.spine.server.entity.storage.ColumnTypeMapping;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.datastore.DatastoreWrapper;
import io.spine.server.storage.datastore.DsColumnMapping;
import io.spine.server.storage.datastore.Kind;
import io.spine.server.storage.datastore.tenant.given.CollegeProjection;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.type.TypeUrl;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;
import static io.spine.base.Identifier.newUuid;

/**
 * A test environment for {@link DsProjectionColumnsTest}.
 */
public final class DsProjectionColumnsTestEnv {

    public static final Class<CollegeProjection> COLLEGE_CLS = CollegeProjection.class;
    public static final Kind COLLEGE_KIND = Kind.of(TypeUrl.from(College.getDescriptor()));

    /**
     * Prevent this test environment from instantiation.
     */
    private DsProjectionColumnsTestEnv() {
    }

    public static com.google.cloud.Timestamp
    writeAndReadDeadline(College college,
                         Version version,
                         ProjectionStorage<CollegeId> storage,
                         DatastoreWrapper datastore,
                         Key key) {
        EntityRecord record = toEntityRecord(college, version);
        CollegeProjection projection = entityWith(college, version);
        EntityRecordWithColumns recordWithCols =
                EntityRecordWithColumns.create(record, projection, storage);
        storage.write(college.getId(), recordWithCols);
        Entity response = datastore.read(key);
        com.google.cloud.Timestamp storedDeadline = readAdmissionDeadline(response);
        return storedDeadline;
    }

    public static College newCollege(CollegeId id, Timestamp admissionDeadline) {
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
        return college;
    }

    private static EntityRecord toEntityRecord(College collegeNoAdmission, Version version) {
        EntityRecord recordNoAdmission = EntityRecord
                .newBuilder()
                .setState(AnyPacker.pack(collegeNoAdmission))
                .setVersion(version)
                .vBuild();
        return recordNoAdmission;
    }

    public static College clearAdmission(College college) {
        return college.toBuilder()
                      .clearAdmissionDeadline()
                      .vBuild();
    }

    private static CollegeProjection entityWith(College state, Version version) {
        CollegeProjection projection =
                Given.projectionOfClass(CollegeProjection.class)
                     .withId(state.getId())
                     .withState(state)
                     .withVersion(version.getNumber())
                     .build();
        return projection;
    }

    public static Version someVersion() {
        return Versions.newVersion(42, Time.currentTime());
    }

    public static Timestamp futureFromNow() {
        return Timestamps.add(Time.currentTime(), Durations.fromDays(100));
    }

    public static CollegeId newId() {
        return CollegeId.newBuilder()
                        .setValue(newUuid())
                        .vBuild();
    }

    private static com.google.cloud.Timestamp readAdmissionDeadline(Entity response) {
        com.google.cloud.Timestamp storedTimestamp = response.getTimestamp(
                College.Column.admissionDeadline()
                              .name()
                              .value());
        return storedTimestamp;
    }

    /**
     * A mapping similar to the default one,
     * but telling to store {@link Timestamp}s as {@code null}s.
     */
    public static final class CustomMapping extends DsColumnMapping {

        @Override
        protected ImmutableMap<Class<?>, ColumnTypeMapping<?, ? extends Value<?>>> customMapping() {
            return ImmutableMap.of(Timestamp.class, ofNullableTimestamp());
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
