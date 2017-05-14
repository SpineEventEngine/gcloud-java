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

package org.spine3.server.storage.datastore.type;

import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.DateTime;
import com.google.cloud.datastore.DateTimeValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.Value;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.spine3.base.Version;
import org.spine3.string.Stringifiers;

import java.util.Date;

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
     * @return new instance of {@link DatastoreColumnType} storing {@link Timestamp} as the
     * {@link DateTime}
     */
    static DatastoreColumnType<Timestamp, DateTime> timestampType() {
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
     * {@code String} representation taken from a {@link org.spine3.string.Stringifier Stringifier}
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
            extends AbstractDatastoreColumnType<Timestamp, DateTime> {

        @Override
        public DateTime convertColumnValue(Timestamp fieldValue) {
            final long millis = Timestamps.toMillis(fieldValue);
            final Date intermediate = new Date(millis);
            return DateTime.copyFrom(intermediate);
        }

        @Override
        public Value<?> toValue(DateTime data) {
            return DateTimeValue.of(data);
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
