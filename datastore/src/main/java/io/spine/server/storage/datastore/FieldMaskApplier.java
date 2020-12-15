/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore;

import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.server.entity.EntityRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.protobuf.Messages.isDefault;
import static io.spine.server.entity.FieldMasks.applyMask;

/**
 * Applies the provided mask to nullable or non-nullable records.
 *
 * <p>Instantiated using one of {@link #nullableRecordMasker(FieldMask) nullableRecordMasker(mask)}
 * or
 * {@link #recordMasker(FieldMask) recordMasker(mask)}.
 */
final class FieldMaskApplier {

    private final FieldMask fieldMask;

    private FieldMaskApplier(FieldMask fieldMask) {
        this.fieldMask = fieldMask;
    }

    /**
     * Creates a {@code Function} which applies the provided {@link FieldMask fieldMask}
     * to the {@link EntityRecord EntityRecords} supplied to it.
     *
     * <p>If the supplied {@code EntityRecord} is {@code null}, the {@code Function} will
     * throw an {@link java.lang.NullPointerException}.
     */
    static Function<EntityRecord, EntityRecord> recordMasker(FieldMask fieldMask) {
        checkNotNull(fieldMask);
        return new FieldMaskApplier(fieldMask)::mask;
    }

    /**
     * Creates a {@code Function} which returns {@code null} if the input was {@code null},
     * applying the provided {@link FieldMask fieldMask} otherwise.
     */
    static Function<@Nullable EntityRecord, @Nullable EntityRecord>
    nullableRecordMasker(FieldMask fieldMask) {
        checkNotNull(fieldMask);
        return new FieldMaskApplier(fieldMask)::maskNullable;
    }

    private @Nullable EntityRecord maskNullable(@Nullable EntityRecord record) {
        if (record == null) {
            return null;
        }
        return mask(record);
    }

    private EntityRecord mask(EntityRecord record) {
        checkNotNull(record);
        if (!isDefault(fieldMask)) {
            return recordMasker(record);
        }
        return record;
    }

    private EntityRecord recordMasker(EntityRecord record) {
        Message state = unpack(record.getState());
        Message maskedState = applyMask(fieldMask, state);
        return record
                .toBuilder()
                .setState(pack(maskedState))
                .vBuild();
    }
}
