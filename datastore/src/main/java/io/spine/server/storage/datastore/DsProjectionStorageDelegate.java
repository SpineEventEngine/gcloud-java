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

package io.spine.server.storage.datastore;

/**
 * A {@link io.spine.server.storage.RecordStorage RecordStorage} to which
 * {@link DsProjectionStorage} delegates its operations.
 *
 * <p>It's required to override specific database connection routines for storing projections.
 * This is done for performance reasons.
 */
public class DsProjectionStorageDelegate<I> extends DsRecordStorage<I> {

    private DsProjectionStorageDelegate(Builder<I> builder) {
        super(builder);
    }

    /**
     * Creates new instance of the {@link Builder}.
     *
     * <p>Not to be confused with {@link DsRecordStorage#newBuilder()}, which creates a builder for
     * a {@code DsRecordStorage}. This method has a different name to avoid method hiding clash.
     *
     * @param <I>
     *         the ID type of the instances built by the created {@link Builder}
     * @return new instance of the {@link Builder}
     */
    public static <I> Builder<I> newDelegateBuilder() {
        return new Builder<>();
    }

    /**
     * A builder for the {@code DsProjectionStorageDelegate}.
     */
    public static final class Builder<I>
            extends RecordStorageBuilder<I, DsProjectionStorageDelegate<I>, Builder<I>> {

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
            super();
        }

        /**
         * Creates new instance of the {@code DsProjectionStorageDelegate}.
         */
        @Override
        public DsProjectionStorageDelegate<I> build() {
            checkRequiredFields();
            DsProjectionStorageDelegate<I> storage =
                    new DsProjectionStorageDelegate<>(this);
            return storage;
        }

        @Override
        Builder<I> self() {
            return this;
        }
    }
}
