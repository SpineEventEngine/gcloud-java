/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import static io.spine.server.entity.FieldMasks.applyMask;
import static io.spine.validate.Validate.isDefault;

class FieldMaskApplier {

    private final FieldMask fieldMask;

    private FieldMaskApplier(FieldMask fieldMask) {
        this.fieldMask = fieldMask;
    }

    static Function<EntityRecord, EntityRecord> maskRecord(FieldMask fieldMask) {
        checkNotNull(fieldMask);
        return new FieldMaskApplier(fieldMask)::mask;
    }

    static Function<@Nullable EntityRecord, @Nullable EntityRecord>
    maskNullableRecord(FieldMask fieldMask) {
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
            return maskRecord(record);
        }
        return record;
    }

    private EntityRecord maskRecord(EntityRecord record) {
        Message state = unpack(record.getState());
        Message maskedState = applyMask(fieldMask, state);
        return EntityRecord.newBuilder(record)
                           .setState(pack(maskedState))
                           .build();
    }
}
