/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import io.spine.annotation.SPI;
import io.spine.core.Version;
import io.spine.server.storage.AbstractColumnMapping;
import io.spine.server.storage.ColumnTypeMapping;
import io.spine.string.Stringifiers;

import java.util.HashMap;
import java.util.Map;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;

/**
 * The default Datastore column mapping.
 *
 * <p>All column values are stored as Datastore {@link Value}-s.
 *
 * <p>Users of the storage can extend this class to specify their own column mapping for the
 * selected types.
 */
public class DsColumnMapping extends AbstractColumnMapping<Value<?>> {

    private static final Map<Class<?>, ColumnTypeMapping<?, ? extends Value<?>>> defaults
            = ImmutableMap.of(Timestamp.class, ofTimestamp(),
                              Duration.class, ofDuration(),
                              Version.class, ofVersion());

    /**
     * {@inheritDoc}
     *
     * <p>Merges the default column mapping rules with those provided by SPI users.
     * In case there are duplicate mappings for some column type, the value provided
     * by SPI users is used.
     *
     * @apiNote This method is made {@code final}, as it is designed
     *         to use {@code ImmutableMap.Builder}, which does not allow to override values.
     *         Therefore, it is not possible for SPI users to provide their own mapping rules
     *         for types such as {@code Timestamp}, for which this class already has
     *         a default mapping. SPI users should override
     *         {@link #customMapping() DsColumnMapping.customMapping()} instead.
     */
    @Override
    protected final void
    setupCustomMapping(
            ImmutableMap.Builder<Class<?>, ColumnTypeMapping<?, ? extends Value<?>>> builder) {
        Map<Class<?>, ColumnTypeMapping<?, ? extends Value<?>>> merged = new HashMap<>();
        var custom = customMapping();
        merged.putAll(defaults);
        merged.putAll(custom);
        builder.putAll(merged);
    }

    /**
     * Returns the custom column mapping rules.
     *
     * <p>This method is designed for SPI users in order to be able to re-define
     * and-or append their custom mapping. As by default, {@code DsColumnMapping}
     * provides rules for {@link Timestamp} and {@link Version}, SPI users may
     * choose to either override these defaults by returning their own mapping for these types,
     * or supply even more mapping rules.
     *
     * <p>By default, this method returns an empty map.
     *
     * @return custom column mappings, per Java class of column
     */
    @SPI
    protected ImmutableMap<Class<?>, ColumnTypeMapping<?, ? extends Value<?>>> customMapping() {
        return ImmutableMap.of();
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
            var blob = Blob.copyFrom(bytes.asReadOnlyByteBuffer());
            return BlobValue.of(blob);
        };
    }

    @Override
    @SuppressWarnings("EnumOrdinal") /* The ordinal is used to store enum values in the database. */
    protected ColumnTypeMapping<Enum<?>, LongValue> ofEnum() {
        return anEnum -> LongValue.of(anEnum.ordinal());
    }

    /**
     * {@inheritDoc}
     *
     * <p>A message marked with the {@code (compare_by)} option is stored as an
     * {@linkplain CompareByEncoder order-preserving string key}, so that it can be used in
     * ordering comparison ({@code >}/{@code <}/{@code >=}/{@code <=}) queries. Any other message
     * is stored using its {@link Stringifiers#toString(Object) string} form, which is suitable
     * for equality but not for ordering.
     */
    @Override
    protected ColumnTypeMapping<Message, StringValue> ofMessage() {
        return msg -> {
            if (CompareByEncoder.isComparable(msg.getDescriptorForType())) {
                return StringValue.of(CompareByEncoder.encode(msg));
            }
            var str = Stringifiers.toString(msg);
            return StringValue.of(str);
        };
    }

    @Override
    public ColumnTypeMapping<?, ? extends Value<?>> ofNull() {
        return o -> NullValue.of();
    }

    /**
     * Returns the default mapping from {@link Timestamp} to {@link TimestampValue}.
     */
    @SuppressWarnings({
            "ProtoTimestampGetSecondsGetNano" /* In order to create exact value. */,
            "UnnecessaryLambda" /* For brevity.*/,
            "WeakerAccess" /* To allow access for SPI users. */})
    protected static ColumnTypeMapping<Timestamp, TimestampValue> ofTimestamp() {
        return timestamp -> TimestampValue.of(
                ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos())
        );
    }

    /**
     * Returns the default mapping from {@link Version} to {@link LongValue}.
     */
    @SuppressWarnings({
            "UnnecessaryLambda" /* For brevity.*/,
            "WeakerAccess" /* To allow access for SPI users. */})
    protected static ColumnTypeMapping<Version, LongValue> ofVersion() {
        return version -> LongValue.of(version.getNumber());
    }

    /**
     * Returns the default mapping from {@link Duration} to {@link LongValue}.
     *
     * <p>The duration is stored as its total number of nanoseconds, which preserves ordering
     * for use in comparison queries. {@code Duration} does not implement {@link Comparable};
     * this mapping reproduces the ordering of the {@code Comparator} registered for it in
     * {@link io.spine.compare.ComparatorRegistry ComparatorRegistry}. Durations beyond about
     * ±292 years do not fit into a {@code long} and are rejected by
     * {@link Durations#toNanos(Duration)}.
     */
    @SuppressWarnings({
            "UnnecessaryLambda" /* For brevity.*/,
            "WeakerAccess" /* To allow access for SPI users. */})
    protected static ColumnTypeMapping<Duration, LongValue> ofDuration() {
        return duration -> LongValue.of(Durations.toNanos(duration));
    }
}
