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
import io.spine.server.entity.storage.AbstractTypeRegistry;
import io.spine.server.entity.storage.PersistenceStrategy;
import io.spine.server.entity.storage.PersistenceStrategyOfNull;
import io.spine.string.Stringifiers;

import java.util.function.Supplier;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;

/**
 * Non-{@code final}, implement to ..., maybe {@link io.spine.annotation.SPI}.
 */
public class DsTypeRegistry extends AbstractTypeRegistry<Value<?>> {

    @Override
    protected void
    setupCustomStrategies(
            ImmutableMap.Builder<Class<?>, Supplier<PersistenceStrategy<?, ? extends Value<?>>>> builder) {
        builder.put(Timestamp.class, DsTypeRegistry::timestampPersistenceStrategy);
        builder.put(Version.class, DsTypeRegistry::versionPersistenceStrategy);
    }

    @Override
    protected PersistenceStrategy<String, StringValue> stringPersistenceStrategy() {
        return StringValue::of;
    }

    @Override
    protected PersistenceStrategy<Integer, LongValue> integerPersistenceStrategy() {
        return LongValue::of;
    }

    @Override
    protected PersistenceStrategy<Long, LongValue> longPersistenceStrategy() {
        return LongValue::of;
    }

    @Override
    protected PersistenceStrategy<Float, DoubleValue> floatPersistenceStrategy() {
        return DoubleValue::of;
    }

    @Override
    protected PersistenceStrategy<Double, DoubleValue> doublePersistenceStrategy() {
        return DoubleValue::of;
    }

    @Override
    protected PersistenceStrategy<Boolean, BooleanValue> booleanPersistenceStrategy() {
        return BooleanValue::of;
    }

    @Override
    protected PersistenceStrategy<ByteString, BlobValue> byteStringPersistenceStrategy() {
        return bytes -> {
            Blob blob = Blob.copyFrom(bytes.asReadOnlyByteBuffer());
            return BlobValue.of(blob);
        };
    }

    @Override
    protected PersistenceStrategy<Enum<?>, LongValue> enumPersistenceStrategy() {
        return anEnum -> LongValue.of(anEnum.ordinal());
    }

    @Override
    protected PersistenceStrategy<Message, StringValue> messagePersistenceStrategy() {
        return msg -> {
            String str = Stringifiers.toString(msg);
            return StringValue.of(str);
        };
    }

    @Override
    public PersistenceStrategyOfNull<NullValue> persistenceStrategyOfNull() {
        return NullValue::of;
    }

    private static PersistenceStrategy<Timestamp, TimestampValue> timestampPersistenceStrategy() {
        return timestamp -> TimestampValue.of(
                ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos())
        );
    }

    private static PersistenceStrategy<Version, LongValue> versionPersistenceStrategy() {
        return version -> LongValue.of(version.getNumber());
    }
}
