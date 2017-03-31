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

import com.google.cloud.datastore.DateTime;
import com.google.cloud.datastore.Entity;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import org.spine3.base.Version;
import org.spine3.json.Json;
import org.spine3.protobuf.Timestamps2;

import java.util.Date;

/**
 * @author Dmytro Dashenkov
 */
public class DsColumnTypes {

    private DsColumnTypes() {
        // Prevent instantiation of a utility class
    }

    public static SimpleDatastoreColumnType<String> stringType() {
        return new StringColumnType();
    }

    public static SimpleDatastoreColumnType<Integer> integerType() {
        return new IntegerColumnType();
    }

    public static SimpleDatastoreColumnType<Boolean> booleanType() {
        return new BooleanColumnType();
    }

    public static DatastoreColumnType<Timestamp, DateTime> timestampType() {
        return new TimestampColumnType();
    }

    public static DatastoreColumnType<AbstractMessage, String> messageType() {
        return new MessageType();
    }

    public static DatastoreColumnType<Version, Integer> versionType() {
        return new VersionColumnType();
    }

    private static class StringColumnType
            extends SimpleDatastoreColumnType<String> {

        @Override
        public void setColumnValue(Entity.Builder storageRecord, String value, String columnIdentifier) {
            storageRecord.set(columnIdentifier, value);
        }
    }

    private static class IntegerColumnType
            extends SimpleDatastoreColumnType<Integer> {

        @Override
        public void setColumnValue(Entity.Builder storageRecord, Integer value, String columnIdentifier) {
            storageRecord.set(columnIdentifier, value);
        }
    }

    private static class BooleanColumnType
            extends SimpleDatastoreColumnType<Boolean> {

        @Override
        public void setColumnValue(Entity.Builder storageRecord, Boolean value, String columnIdentifier) {
            storageRecord.set(columnIdentifier, value);
        }
    }

    private static class TimestampColumnType implements DatastoreColumnType<Timestamp, DateTime> {

        @Override
        public DateTime convertColumnValue(Timestamp fieldValue) {
            final Date intermediate = new Date(fieldValue.getNanos() / Timestamps2.NANOS_PER_MILLISECOND);
            return DateTime.copyFrom(intermediate);
        }

        @Override
        public void setColumnValue(Entity.Builder storageRecord, DateTime value, String columnIdentifier) {
            storageRecord.set(columnIdentifier, value);
        }
    }

    private static class VersionColumnType implements DatastoreColumnType<Version, Integer> {

        @Override
        public Integer convertColumnValue(Version fieldValue) {
            return fieldValue.getNumber();
        }

        @Override
        public void setColumnValue(Entity.Builder storageRecord, Integer value, String columnIdentifier) {
            storageRecord.set(columnIdentifier, value);
        }
    }

    private static class MessageType implements DatastoreColumnType<AbstractMessage, String> {

        @Override
        public String convertColumnValue(AbstractMessage fieldValue) {
            return Json.toJson(fieldValue);
        }

        @Override
        public void setColumnValue(Entity.Builder storageRecord, String value, String columnIdentifier) {
            storageRecord.set(columnIdentifier, value);
        }
    }
}
