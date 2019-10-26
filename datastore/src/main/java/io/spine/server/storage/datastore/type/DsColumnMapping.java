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

import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.BlobValue;
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;
import io.spine.server.entity.storage.AbstractColumnMapping;
import io.spine.server.entity.storage.ColumnTypeMapping;
import io.spine.string.Stringifiers;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;

/**
 * Non-{@code final}, implement to ..., maybe {@link io.spine.annotation.SPI}.
 */
public class DsColumnMapping extends AbstractColumnMapping<Value<?>> {

    @Override
    protected void
    setupCustomMapping(
            ImmutableMap.Builder<Class<?>, ColumnTypeMapping<?, ? extends Value<?>>> builder) {
        builder.put(Timestamp.class, ofTimestamp());
        builder.put(Version.class, ofVersion());
    }

    @Override
    protected ColumnTypeMapping<String, StringValue> ofString() {
        return StringValue::of;
    }

    @Override
    protected ColumnTypeMapping<Integer, LongValue> ofInteger() {
        return LongValue::of;
    }

    @Override
    protected ColumnTypeMapping<Long, LongValue> ofLong() {
        return LongValue::of;
    }

    @Override
    protected ColumnTypeMapping<Float, DoubleValue> ofFloat() {
        return DoubleValue::of;
    }

    @Override
    protected ColumnTypeMapping<Double, DoubleValue> ofDouble() {
        return DoubleValue::of;
    }

    @Override
    protected ColumnTypeMapping<Boolean, BooleanValue> ofBoolean() {
        return BooleanValue::of;
    }

    @Override
    protected ColumnTypeMapping<ByteString, BlobValue> ofByteString() {
        return bytes -> {
            Blob blob = Blob.copyFrom(bytes.asReadOnlyByteBuffer());
            return BlobValue.of(blob);
        };
    }

    @Override
    protected ColumnTypeMapping<Enum<?>, LongValue> ofEnum() {
        return anEnum -> LongValue.of(anEnum.ordinal());
    }

    @Override
    protected ColumnTypeMapping<Message, StringValue> ofMessage() {
        return msg -> {
            String str = Stringifiers.toString(msg);
            return StringValue.of(str);
        };
    }

    @Override
    public ColumnTypeMapping<?, ? extends Value<?>> ofNull() {
        return o -> NullValue.of();
    }

    @SuppressWarnings("ProtoTimestampGetSecondsGetNano") // This behavior is intended.
    private static ColumnTypeMapping<Timestamp, TimestampValue> ofTimestamp() {
        return timestamp -> TimestampValue.of(
                ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos())
        );
    }

    private static ColumnTypeMapping<Version, LongValue> ofVersion() {
        return version -> LongValue.of(version.getNumber());
    }
}
