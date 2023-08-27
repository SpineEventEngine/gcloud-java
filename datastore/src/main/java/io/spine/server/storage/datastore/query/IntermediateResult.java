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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * The result obtained from Datastore directly by sending one or more queries to it.
 *
 * <p>In order to be returned as a lookup result, needs to be post-processed in memory.
 */
final class IntermediateResult {

    private final List<@Nullable Entity> entities;

    /**
     * Creates a new instance by referencing (not copying) the given list of Entities.
     *
     * @param entities
     *         list of Datastore entities, some of which may be {@code null} in case
     *         they were queried by identifiers, and the requested records
     *         were missing from the underlying storage
     * @apiNote This ctor does not utilize an {@code ImmutableList},
     *         as it cannot contain {@code null}s.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")  /* To improve performance. */
    IntermediateResult(List<@Nullable Entity> entities) {
        this.entities = entities;
    }

    /**
     * Returns the list of entities being an intermediate result of querying.
     *
     * <p>The obtained list may contain {@code null}s if the records requested by IDs were
     * missing in the storage.
     *
     * <p>The returned list is unmodifiable.
     *
     * @apiNote Guava's {@code ImmutableList} is not applicable here, as it isn't capable of
     *         holding {@code null} values
     */
    List<@Nullable Entity> entities() {
        return unmodifiableList(entities);
    }
}
