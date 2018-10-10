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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import io.spine.type.TypeUrl;

import static io.spine.server.storage.datastore.DsFilters.activeEntity;

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
     * {@inheritDoc}
     *
     * <p>{@code DsProjectionStorageDelegate} implementation takes advantage of
     * {@link io.spine.server.projection.Projection Projection} being an
     * {@link io.spine.server.entity.EntityWithLifecycle EntityWithLifecycle} by filtering out
     * the non-active entities on the database side.
     *
     * @see DsFilters#activeEntity() the active entity filter
     */
    @Override
    protected StructuredQuery<Entity> buildAllQuery(TypeUrl typeUrl) {
        String entityKind = kindFrom(typeUrl).getValue();
        StructuredQuery<Entity> query =
                Query.newEntityQueryBuilder()
                     .setKind(entityKind)
                     .setFilter(activeEntity())
                     .build();
        return query;
    }

    /**
     * Creates new instance of the {@link Builder}.
     *
     * <p>Not to be confused with {@link DsRecordStorage#newBuilder()}, which creates a builder for
     * a {@code DsRecordStorage}. This method has a different name to avoid method hiding clash.
     *
     * @param <I> the ID type of the instances built by the created {@link Builder}
     * @return new instance of the {@link Builder}
     */
    public static <I> Builder<I> newDelegateBuilder() {
        return new Builder<>();
    }

    /**
     * A builder for the {@code DsProjectionStorageDelegate}.
     */
    public static final class Builder<I> extends RecordStorageBuilder<I, Builder<I>> {

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
            super();
        }

        /**
         * Creates new instance of the {@code DsProjectionStorageDelegate}.
         */
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
