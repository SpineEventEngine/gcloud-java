/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore.query;

import com.google.cloud.datastore.Entity;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.server.storage.datastore.record.Entities;
import io.spine.server.storage.datastore.record.FieldMaskApplier;
import io.spine.type.TypeUrl;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Filters the contents of {@link IntermediateResult} and converts them
 * to the {@code R}-typed records.
 *
 * @param <R>
 *         the type of records
 */
abstract class ToRecords<R extends Message> implements Function<IntermediateResult, Iterable<R>> {

    private final TypeUrl recordType;
    private final Function<R, R> masker;

    /**
     * Creates an instance of this function.
     *
     * @param type
     *         a type URL of records to convert
     * @param mask
     *         a field mask to apply to each record during conversion
     */
    ToRecords(TypeUrl type, FieldMask mask) {
        recordType = type;
        this.masker = FieldMaskApplier.recordMasker(mask);
    }

    @Override
    public Iterable<R> apply(IntermediateResult result) {
        var entities = result.entities();
        var stream = entities.stream().filter(Objects::nonNull);
        stream = filter(stream);
        @SuppressWarnings("ConstantConditions") /* `null` were already filtered out. */
        var records =
                stream.map(this::toRecord)
                      .map(masker)
                      .collect(toImmutableList());
        return records;
    }

    private R toRecord(Entity e) {
        return Entities.toMessage(e, recordType);
    }

    /**
     * Filters the entities.
     *
     * <p>Descendant types are expected to provide their own rules for filtering.
     */
    protected abstract Stream<Entity> filter(Stream<Entity> entities);
}
