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

package io.spine.server.storage.datastore.type;

import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;
import io.spine.string.Stringifiers;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;

/**
 * A utility for creating the basic {@link DatastoreColumnType} implementations for
 * <ul>
 *     <li>{@code String}
 *     <li>{@code Integer}
 *     <li>{@code Boolean}
 *     <li>{@link Timestamp}
 *     <li>{@link Version}
 *     <li>{@link AbstractMessage}
 * </ul>
 *
 * @author Dmytro Dashenkov
 */
final class DsColumnTypes {

    private DsColumnTypes() {
        // Prevent instantiation of a utility class
    }

    /**
     * @return new instance of {@link SimpleDatastoreColumnType SimpleDatastoreColumnType<String>}
     */
    static SimpleDatastoreColumnType<String> stringType() {
        return new StringColumnType();
    }

    /**
     * @return new instance of {@link SimpleDatastoreColumnType SimpleDatastoreColumnType<Integer>}
     */
    static SimpleDatastoreColumnType<Integer> integerType() {
        return new IntegerColumnType();
    }

    /**
     * @return new instance of {@link SimpleDatastoreColumnType SimpleDatastoreColumnType<Long>}
     */
    static SimpleDatastoreColumnType<Long> longType() {
        return new LongColumnType();
    }

    /**
     * @return new instance of {@link SimpleDatastoreColumnType SimpleDatastoreColumnType<Boolean>}
     */
    static SimpleDatastoreColumnType<Boolean> booleanType() {
        return new BooleanColumnType();
    }

    /**
     * @return new instance of {@link DatastoreColumnType} storing Protobuf {@link Timestamp} as the
     * Datastore {@link com.google.cloud.Timestamp Timestamp}
     */
    static DatastoreColumnType<Timestamp, com.google.cloud.Timestamp> timestampType() {
        return new TimestampColumnType();
    }

    /**
     * @return new instance of {@link DatastoreColumnType} storing {@link Version} as the version
     * {@link Version#getNumber() number}.
     */
    static DatastoreColumnType<Version, Integer> versionType() {
        return new VersionColumnType();
    }

    /**
     * @return new instance of {@link DatastoreColumnType} storing {@link AbstractMessage} as its
     * {@code String} representation taken from a {@link io.spine.string.Stringifier Stringifier}
     */
    static DatastoreColumnType<AbstractMessage, String> messageType() {
        return new MessageType();
    }

    private static class StringColumnType
            extends SimpleDatastoreColumnType<String> {

        @Override
        public Value<?> toValue(String data) {
            return StringValue.of(data);
        }
    }

    private static class IntegerColumnType
            extends SimpleDatastoreColumnType<Integer> {

        @Override
        public Value<?> toValue(Integer data) {
            return LongValue.of(data);
        }
    }

    private static class LongColumnType
            extends SimpleDatastoreColumnType<Long> {

        @Override
        public Value<?> toValue(Long data) {
            return LongValue.of(data);
        }
    }

    private static class BooleanColumnType
            extends SimpleDatastoreColumnType<Boolean> {

        @Override
        public Value<?> toValue(Boolean data) {
            return BooleanValue.of(data);
        }
    }

    private static class TimestampColumnType
            extends AbstractDatastoreColumnType<Timestamp, com.google.cloud.Timestamp> {

        @Override
        public com.google.cloud.Timestamp convertColumnValue(Timestamp fieldValue) {
            return ofTimeSecondsAndNanos(fieldValue.getSeconds(), fieldValue.getNanos());
        }

        @Override
        public Value<?> toValue(com.google.cloud.Timestamp data) {
            return TimestampValue.of(data);
        }
    }

    private static class VersionColumnType
            extends AbstractDatastoreColumnType<Version, Integer> {

        @Override
        public Integer convertColumnValue(Version fieldValue) {
            return fieldValue.getNumber();
        }

        @Override
        public Value<?> toValue(Integer data) {
            return LongValue.of(data);
        }
    }

    private static class MessageType
            extends AbstractDatastoreColumnType<AbstractMessage, String> {

        @Override
        public String convertColumnValue(AbstractMessage fieldValue) {
            return Stringifiers.toString(fieldValue);
        }

        @Override
        public Value<?> toValue(String data) {
            return StringValue.of(data);
        }
    }
}
