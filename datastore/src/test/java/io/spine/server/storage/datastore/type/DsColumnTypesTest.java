/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;
import io.spine.core.Versions;
import io.spine.json.Json;
import io.spine.server.storage.datastore.given.TestDatastores;
import io.spine.test.storage.Project;
import io.spine.testdata.Sample;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Time.currentTime;
import static io.spine.server.storage.datastore.type.DsColumnTypes.timestampType;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;

@DisplayName("DsColumnTypes should")
class DsColumnTypesTest {

    private static final String COLUMN_LABEL = "some-column";
    private BaseEntity.Builder<Key, Entity.Builder> entityBuilder;

    @BeforeEach
    void setUp() {
        entityBuilder = entityBuilder();
    }

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void testPrivateCtor() {
        assertHasPrivateParameterlessCtor(DsColumnTypes.class);
    }

    @Test
    @DisplayName("provide simple string type")
    void testString() {
        SimpleDatastoreColumnType<String> type = DsColumnTypes.stringType();
        String value = "some string";

        setSimpleType(type, value);

        BaseEntity<Key> entity = entityBuilder.build();
        String entityField = entity.getString(COLUMN_LABEL);

        assertThat(entityField).isEqualTo(value);
    }

    @Test
    @DisplayName("provide simple int type")
    void testInt() {
        SimpleDatastoreColumnType<Integer> type = DsColumnTypes.integerType();
        int value = 42;

        setSimpleType(type, value);

        BaseEntity<Key> entity = entityBuilder.build();
        long entityField = entity.getLong(COLUMN_LABEL);

        assertThat(entityField).isEqualTo(value);
    }

    @Test
    @DisplayName("provide simple float type")
    void testFloat() {
        SimpleDatastoreColumnType<Float> type = DsColumnTypes.floatType();
        float value = 3.14f;

        setSimpleType(type, value);

        BaseEntity<Key> entity = entityBuilder.build();
        double entityField = entity.getDouble(COLUMN_LABEL);

        assertThat(entityField)
                .isWithin(0.01)
                .of(value);
    }

    @Test
    @DisplayName("provide simple double type")
    void testDouble() {
        SimpleDatastoreColumnType<Double> type = DsColumnTypes.doubleType();
        double value = 2.718281828459045;

        setSimpleType(type, value);

        BaseEntity<Key> entity = entityBuilder.build();
        double entityField = entity.getDouble(COLUMN_LABEL);

        assertThat(entityField)
                .isWithin(0.01)
                .of(value);
    }

    @Test
    @DisplayName("provide simple long type")
    void testLong() {
        SimpleDatastoreColumnType<Long> type = DsColumnTypes.longType();
        long value = 42L;

        setSimpleType(type, value);

        BaseEntity<Key> entity = entityBuilder.build();
        long entityField = entity.getLong(COLUMN_LABEL);

        assertThat(entityField).isEqualTo(value);
    }

    @Test
    @DisplayName("provide simple boolean type")
    void testBoolean() {
        SimpleDatastoreColumnType<Boolean> type = DsColumnTypes.booleanType();
        boolean value = true;

        setSimpleType(type, value);

        BaseEntity<Key> entity = entityBuilder.build();
        boolean entityField = entity.getBoolean(COLUMN_LABEL);

        assertThat(entityField).isEqualTo(value);
    }

    @Test
    @DisplayName("provide Timestamp as DateTime type")
    void testTimestampToDateTime() {
        DatastoreColumnType<Timestamp, com.google.cloud.Timestamp> type = timestampType();
        Timestamp value = currentTime();

        com.google.cloud.Timestamp timestamp = ofTimeSecondsAndNanos(value.getSeconds(),
                                                                     value.getNanos());

        setDatastoreType(type, value, timestamp);

        BaseEntity<Key> entity = entityBuilder.build();
        com.google.cloud.Timestamp entityField = entity.getTimestamp(COLUMN_LABEL);

        assertThat(entityField).isEqualTo(timestamp);
    }

    @Test
    @DisplayName("provide Version as int type")
    void testVersionToInt() {
        DatastoreColumnType<Version, Integer> type = DsColumnTypes.versionType();
        Version value = Versions.zero();
        value = Versions.increment(value);
        int number = 1;

        setDatastoreType(type, value, number);

        BaseEntity<Key> entity = entityBuilder.build();
        long entityField = entity.getLong(COLUMN_LABEL);

        assertThat(entityField).isEqualTo(number);
    }

    @Test
    @DisplayName("provide Message as String type")
    void testMessageToString() {
        DatastoreColumnType<AbstractMessage, String> type = DsColumnTypes.messageType();

        AbstractMessage value = Sample.messageOfType(Project.class);
        String stringMessage = Json.toCompactJson(value); // default Stringifier behavior

        setDatastoreType(type, value, stringMessage);

        BaseEntity<Key> entity = entityBuilder.build();
        String entityField = entity.getString(COLUMN_LABEL);

        assertThat(entityField).isEqualTo(stringMessage);
    }

    @Test
    @DisplayName("set null value")
    void testNull() {
        SimpleDatastoreColumnType<Boolean> type = DsColumnTypes.booleanType();
        type.setNull(entityBuilder, COLUMN_LABEL);

        BaseEntity<Key> entity = entityBuilder.build();
        String entityField = entity.getString(COLUMN_LABEL);

        assertThat(entityField).isNull();
    }

    private <T> void setSimpleType(SimpleDatastoreColumnType<T> type, T value) {
        T storedValue = type.convertColumnValue(value);
        assertThat(storedValue).isEqualTo(value);

        type.setColumnValue(entityBuilder, storedValue, COLUMN_LABEL);
    }

    private <J, S> void setDatastoreType(DatastoreColumnType<J, S> type,
                                         J value,
                                         S expectedStoredValue) {
        S storedValue = type.convertColumnValue(value);
        assertThat(storedValue).isEqualTo(expectedStoredValue);

        type.setColumnValue(entityBuilder, storedValue, COLUMN_LABEL);
    }

    private static BaseEntity.Builder<Key, Entity.Builder> entityBuilder() {
        String projectId = TestDatastores.projectId()
                                         .value();
        Key key = Key.newBuilder(projectId, "some-entity-kind", "some-name")
                     .build();
        Entity.Builder builder = Entity.newBuilder(key);
        return builder;
    }
}
