/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore.record;

import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.protobuf.Messages.isDefault;
import static io.spine.server.entity.FieldMasks.applyMask;

/**
 * Applies the provided mask to Protobuf messages.
 */
public final class FieldMaskApplier {

    private final FieldMask fieldMask;

    private FieldMaskApplier(FieldMask fieldMask) {
        this.fieldMask = fieldMask;
    }

    /**
     * Creates a {@code Function} which applies the provided {@link FieldMask fieldMask}
     * to the Protobuf messages stored in Datastore as records.
     */
    public static <R extends Message> Function<R, R> recordMasker(FieldMask fieldMask) {
        checkNotNull(fieldMask);
        return new FieldMaskApplier(fieldMask)::mask;
    }

    private <R extends Message> R mask(R record) {
        checkNotNull(record);
        if (!isDefault(fieldMask)) {
            return recordMasker(record);
        }
        return record;
    }

    private <R extends Message> R recordMasker(R record) {
        var result = applyMask(fieldMask, record);
        return result;
    }
}
