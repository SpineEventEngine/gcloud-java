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

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.DateTime;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.junit.Before;
import org.junit.Test;
import org.spine3.base.Version;
import org.spine3.base.Versions;
import org.spine3.json.Json;
import org.spine3.test.storage.Project;
import org.spine3.testdata.Sample;
import org.spine3.time.Time;

import java.util.Date;

import static com.google.cloud.datastore.BooleanValue.of;
import static com.google.cloud.datastore.DateTimeValue.of;
import static com.google.cloud.datastore.LongValue.of;
import static com.google.cloud.datastore.StringValue.of;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.spine3.test.Tests.assertHasPrivateParameterlessCtor;

/**
 * @author Dmytro Dashenkov
 */
public class DsColumnTypesShould {

    private static final String RANDOM_COLUMN_LABEL = "some-column";
    private BaseEntity.Builder<Key, Entity.Builder> entity;

    @Before
    public void setUp() {
        entity = mockEntity();
    }

    @Test
    public void have_private_utility_ctor() {
        assertHasPrivateParameterlessCtor(DsColumnTypes.class);
    }

    @Test
    public void provide_simple_string_type() {
        final SimpleDatastoreColumnType<String> type = DsColumnTypes.stringType();
        final String value = "some string";

        setSimpleType(type, value);

        verify(entity).set(eq(RANDOM_COLUMN_LABEL), eq(of(value)));
    }

    @Test
    public void provide_simple_int_type() {
        final SimpleDatastoreColumnType<Integer> type = DsColumnTypes.integerType();
        final int value = 42;

        setSimpleType(type, value);

        verify(entity).set(eq(RANDOM_COLUMN_LABEL), eq(of(value)));
    }

    @Test
    public void provide_simple_long_type() {
        final SimpleDatastoreColumnType<Long> type = DsColumnTypes.longType();
        final long value = 42L;

        setSimpleType(type, value);

        verify(entity).set(eq(RANDOM_COLUMN_LABEL), eq(of(value)));
    }

    @Test
    public void provide_simple_bool_type() {
        final SimpleDatastoreColumnType<Boolean> type = DsColumnTypes.booleanType();
        final boolean value = true;

        setSimpleType(type, value);

        verify(entity).set(eq(RANDOM_COLUMN_LABEL), eq(of(value)));
    }

    @Test
    public void provide_timestamp_to_date_time_type() {
        final DatastoreColumnType<Timestamp, DateTime> type = DsColumnTypes.timestampType();
        final Timestamp value = Time.getCurrentTime();

        final Date date = new Date(Timestamps.toMillis(value));
        final DateTime dateTime = DateTime.copyFrom(date);

        setDatastoreType(type, value, dateTime);

        verify(entity).set(eq(RANDOM_COLUMN_LABEL), eq(of(dateTime)));
    }

    @Test
    public void provide_version_to_int_type() {
        final DatastoreColumnType<Version, Integer> type = DsColumnTypes.versionType();
        Version value = Versions.create();
        value = Versions.increment(value);
        final int number = 1;


        setDatastoreType(type, value, number);

        verify(entity).set(eq(RANDOM_COLUMN_LABEL), eq(of(number)));
    }

    @Test
    public void provide_message_to_string_type() {
        final DatastoreColumnType<AbstractMessage, String> type = DsColumnTypes.messageType();

        final AbstractMessage value = Sample.messageOfType(Project.class);
        final String stringMessage = Json.toCompactJson(value); // default Stringifier behavior

        setDatastoreType(type, value, stringMessage);

        verify(entity).set(eq(RANDOM_COLUMN_LABEL), eq(of(stringMessage)));
    }

    @Test
    public void set_null_value() {
        final SimpleDatastoreColumnType<Boolean> type = DsColumnTypes.booleanType();
        type.setNull(entity, RANDOM_COLUMN_LABEL);
        verify(entity).setNull(eq(RANDOM_COLUMN_LABEL));
    }

    private <T> void setSimpleType(SimpleDatastoreColumnType<T> type, T value) {
        final T storedValue = type.convertColumnValue(value);
        assertEquals(value, storedValue);

        type.setColumnValue(entity, storedValue, RANDOM_COLUMN_LABEL);
    }

    private <J, S> void setDatastoreType(DatastoreColumnType<J, S> type,
                                         J value,
                                         S expectedStoredValue) {
        final S storedValue = type.convertColumnValue(value);
        assertEquals(expectedStoredValue, storedValue);

        type.setColumnValue(entity, storedValue, RANDOM_COLUMN_LABEL);
    }

    @SuppressWarnings("unchecked") // Because of mocking
    private static BaseEntity.Builder<Key, Entity.Builder> mockEntity() {
        return mock(BaseEntity.Builder.class);
    }
}
