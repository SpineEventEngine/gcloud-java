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

package io.spine.server.storage.datastore.config;

import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.BlobValue;
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.protobuf.ByteString;
import io.spine.base.Time;
import io.spine.core.Version;
import io.spine.string.Stringifiers;
import io.spine.test.storage.StgProject;
import io.spine.test.storage.StgProject.Status;
import io.spine.test.storage.StgProjectId;
import io.spine.testing.TestValues;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Time.currentTime;
import static java.nio.charset.StandardCharsets.UTF_8;

@DisplayName("`DsColumnMapping` should")
final class DsColumnMappingTest {

    private final DsColumnMapping mapping = new DsColumnMapping();

    @Nested
    @DisplayName("persist")
    class Persist {

        @Test
        @DisplayName("`String` as `StringValue`")
        void stringAsStringValue() {
            var str = "test-string";
            assertConverts(str, StringValue.of(str));
        }

        @Test
        @DisplayName("`Integer` as `LongValue`")
        void integerAsLongValue() {
            var num = 42;
            assertConverts(num, LongValue.of(num));
        }

        @Test
        @DisplayName("`Long` as `LongValue`")
        void longAsLongValue() {
            var num = 42L;
            assertConverts(num, LongValue.of(num));
        }

        @Test
        @DisplayName("`Float` as `DoubleValue`")
        void floatAsDoubleValue() {
            var num = 42.0F;
            assertConverts(num, DoubleValue.of(num));
        }

        @Test
        @DisplayName("`Double` as `DoubleValue`")
        void doubleAsDoubleValue() {
            var num = 42.0;
            assertConverts(num, DoubleValue.of(num));
        }

        @Test
        @DisplayName("`Boolean` as `BooleanValue`")
        void booleanAsBooleanValue() {
            var value = false;
            assertConverts(value, BooleanValue.of(value));
        }

        @Test
        @DisplayName("`ByteString` as `BlobValue`")
        void byteStringAsBlobValue() {
            var str = "some-test-string";
            var bytes = ByteString.copyFromUtf8(str);
            var blob = Blob.copyFrom(str.getBytes(UTF_8));
            var blobValue = BlobValue.of(blob);
            assertConverts(bytes, blobValue);
        }

        @Test
        @DisplayName("`Enum` as `LongValue`")
        void enumAsLongValue() {
            var value = Status.CREATED;
            assertConverts(value, LongValue.of(value.getNumber()));
        }

        @Test
        @DisplayName("generic `Message` as `StringValue`")
        void messageAsStringValue() {
            StgProjectId id = StgProjectId.newBuilder().setId("ID").build();
            var project = StgProject.newBuilder()
                    .setId(id)
                    .setName("project-name")
                    .build();
            var messageAsString = Stringifiers.toString(project);
            assertConverts(project, StringValue.of(messageAsString));
        }

        @Test
        @DisplayName("`Timestamp` as `TimestampValue`")
        void timestampAsTimestampValue() {
            var timestamp = currentTime();
            assertConverts(timestamp, TimestampValue.of(
                    ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos())
            ));
        }

        @Test
        @DisplayName("`Version` as `LongValue`")
        void versionAsLongValue() {
            var number = 42;
            var version = Version.newBuilder()
                    .setNumber(number)
                    .setTimestamp(currentTime())
                    .build();
            assertConverts(version, LongValue.of(number));
        }

        @Test
        @DisplayName("`null` as `NullValue`")
        void nullAsNullValue() {
            var persistedNull = mapping.ofNull()
                                       .applyTo(TestValues.nullRef());
            assertThat(persistedNull).isEqualTo(NullValue.of());
        }

        private void assertConverts(Object original, Value<?> expected) {
            var rule = mapping.of(original.getClass());
            var result = rule.applyTo(original);
            assertThat(result).isEqualTo(expected);
        }
    }
}
