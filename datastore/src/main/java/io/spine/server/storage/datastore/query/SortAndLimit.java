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

package io.spine.server.storage.datastore.query;

import com.google.cloud.datastore.Entity;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.query.SortBy;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.stream.Stream;

import static io.spine.server.storage.datastore.record.DsEntityComparator.implementing;

/**
 * Sorts and limits the original list of {@code Entity} objects, then converts each of them
 * to the {@code <R>}-typed records and applies the specified field mask to each of them.
 *
 * @param <R>
 *         the type of the records to convert each {@code Entity} into
 */
final class SortAndLimit<R extends Message> extends ToRecords<R> {

    /**
     * A limit value which isn't set.
     */
    private static final int UNSET_LIMIT = 0;

    private final ImmutableList<SortBy<?, R>> sorting;
    private final @Nullable Integer limit;

    /**
     * Creates a new instance of this conversion function.
     *
     * @param type
     *         the type of converted records
     * @param mask
     *         a field mask to apply to each record
     * @param sorting
     *         the directives to use for sorting
     * @param limit
     *         if set, a maximum number of records to pass on, in the ascending order of sorting
     */
    SortAndLimit(TypeUrl type, FieldMask mask,
                 ImmutableList<SortBy<?, R>> sorting,
                 @Nullable Integer limit) {
        super(type, mask);
        this.sorting = sorting;
        this.limit = limit;
    }

    @Override
    protected Stream<Entity> filter(Stream<Entity> entities) {
        var currentStream = entities;
        if (!sorting.isEmpty()) {
            currentStream = currentStream.sorted(implementing(sorting));
        }
        if (limit != null && limit != UNSET_LIMIT) {
            currentStream = currentStream.limit(limit);
        }
        return currentStream;
    }
}
