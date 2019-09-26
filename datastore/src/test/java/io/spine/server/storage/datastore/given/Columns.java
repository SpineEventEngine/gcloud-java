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

package io.spine.server.storage.datastore.given;

import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;
import io.spine.core.Versions;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.datastore.type.SimpleDatastoreColumnType;

import java.lang.reflect.Method;

@SuppressWarnings({"ClassWithTooManyMethods", "unused"})
// Provides various types of entity columns.
public final class Columns {

    /** Prevents instantiation of this test env class. */
    private Columns() {
    }

    public static EntityColumn stringColumn() {
        return column("getString");
    }

    public static EntityColumn intColumn() {
        return column("getInt");
    }

    public static EntityColumn longColumn() {
        return column("getLong");
    }

    public static EntityColumn floatColumn() {
        return column("getFloat");
    }

    public static EntityColumn doubleColumn() {
        return column("getDouble");
    }

    public static EntityColumn booleanColumn() {
        return column("getBoolean");
    }

    public static EntityColumn messageColumn() {
        return column("getMessage");
    }

    public static EntityColumn timestampColumn() {
        return column("getTimestamp");
    }

    public static EntityColumn versionColumn() {
        return column("getVersion");
    }

    public static EntityColumn byteColumn() {
        return column("getByte");
    }

    @Column
    public String getString() {
        return "42";
    }

    @Column
    public int getInt() {
        return 42;
    }

    @Column
    public long getLong() {
        return 42L;
    }

    @Column
    public float getFloat() {
        return 42.0F;
    }

    @Column
    public double getDouble() {
        return 42.0;
    }

    @Column
    public boolean getBoolean() {
        return true;
    }

    @Column
    public StringValue getMessage() {
        return StringValue
                .newBuilder()
                .setValue("42")
                .build();
    }

    @Column
    public Timestamp getTimestamp() {
        return Timestamp
                .newBuilder()
                .setSeconds(42)
                .setNanos(42)
                .build();
    }

    @Column
    public Version getVersion() {
        return Versions.zero();
    }

    @Column
    public Byte getByte() {
        return 42;
    }

    private static EntityColumn column(String name) {
        try {
            Method method = Columns.class.getDeclaredMethod(name);
            EntityColumn column = EntityColumn.from(method);
            return column;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    public static final class ByteColumnType extends SimpleDatastoreColumnType<Byte> {

        @Override
        public Value<?> toValue(Byte data) {
            return LongValue.of(data);
        }
    }
}
