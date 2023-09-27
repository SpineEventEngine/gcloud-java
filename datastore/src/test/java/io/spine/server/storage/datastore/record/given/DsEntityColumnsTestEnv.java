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

package io.spine.server.storage.datastore.record.given;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.spine.base.Identifier;
import io.spine.base.Time;
import io.spine.core.Version;
import io.spine.core.Versions;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordStorage;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.ColumnTypeMapping;
import io.spine.server.storage.datastore.DatastoreWrapper;
import io.spine.server.storage.datastore.Kind;
import io.spine.server.storage.datastore.config.DsColumnMapping;
import io.spine.server.storage.datastore.given.CollegeProjection;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.type.TypeUrl;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;
import static com.google.common.base.Preconditions.checkArgument;
import static io.spine.base.Identifier.newUuid;
import static io.spine.protobuf.AnyPacker.pack;

/**
 * Test environment
 * for {@link io.spine.server.storage.datastore.record.DsEntityColumnsTest DsEntityColumnsTest}.
 */
public final class DsEntityColumnsTestEnv {

    public static final Class<CollegeProjection> COLLEGE_CLS = CollegeProjection.class;
    public static final Kind COLLEGE_KIND = Kind.of(TypeUrl.from(College.getDescriptor()));

    /**
     * Prevents this test environment from instantiation.
     */
    private DsEntityColumnsTestEnv() {
    }

    public static com.google.cloud.Timestamp
    writeAndReadDeadline(College college,
                         Version version,
                         EntityRecordStorage<CollegeId, College> storage,
                         DatastoreWrapper datastore,
                         Key key) {
        var record = toEntityRecord(college, version);
        var recordWithCols = EntityRecordWithColumns.create(record, COLLEGE_CLS);
        storage.write(recordWithCols);
        var response = datastore.read(key);
        checkArgument(response.isPresent());
        var storedDeadline = readAdmissionDeadline(response.get());
        return storedDeadline;
    }

    public static College newCollege(CollegeId id, Timestamp admissionDeadline) {
        var college =
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
                        .build();
        return college;
    }

    private static EntityRecord toEntityRecord(College college, Version version) {
        var packedId = Identifier.pack(college.getId());
        var recordNoAdmission = EntityRecord
                .newBuilder()
                .setEntityId(packedId)
                .setState(pack(college))
                .setVersion(version)
                .build();
        return recordNoAdmission;
    }

    public static College clearAdmission(College college) {
        return college.toBuilder()
                .clearAdmissionDeadline()
                .build();
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
                .build();
    }

    private static com.google.cloud.Timestamp readAdmissionDeadline(Entity response) {
        var storedTimestamp = response.getTimestamp(
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

        @SuppressWarnings("UnnecessaryLambda" /* For brevity */)
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
