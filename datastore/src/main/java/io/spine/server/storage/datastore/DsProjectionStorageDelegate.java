/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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
import com.google.protobuf.Descriptors.Descriptor;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.datastore.type.DatastoreColumnType;
import io.spine.type.TypeUrl;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.datastore.DsFilters.activeEntity;

/**
 * A {@link io.spine.server.storage.RecordStorage RecordStorage} to which
 * {@link DsProjectionStorage} delegates its operations.
 *
 * @author Dmytro Dashenkov
 */
public class DsProjectionStorageDelegate<I> extends DsRecordStorage<I> {

    protected DsProjectionStorageDelegate(
            Descriptor descriptor,
            DatastoreWrapper datastore,
            boolean multitenant,
            ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry,
            Class<I> idClass) {
        super(descriptor, datastore, multitenant, columnTypeRegistry, idClass);
    }

    private DsProjectionStorageDelegate(Builder<I> builder) {
        this(builder.descriptor,
             builder.datastore,
             builder.multitenant,
             builder.columnTypeRegistry,
             builder.idClass);
    }

    @Override
    protected StructuredQuery<Entity> buildAllQuery(TypeUrl typeUrl) {
        final String entityKind = kindFrom(typeUrl).getValue();
        final StructuredQuery<Entity> query = Query.newEntityQueryBuilder()
                                                   .setKind(entityKind)
                                                   .setFilter(activeEntity())
                                                   .build();
        return query;
    }

    public static <I> Builder<I> builder() {
        return new Builder<>();
    }

    /**
     * A builder for the {@code DsProjectionStorageDelegate}.
     */
    public static class Builder<I> {

        private Descriptor descriptor;
        private DatastoreWrapper datastore;
        private boolean multitenant;
        private ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry;
        private Class<I> idClass;

        private Builder() {
            // Prevent direct initialization.
        }

        public Builder<I> setStateType(TypeUrl stateTypeUrl) {
            checkNotNull(stateTypeUrl);
            final Descriptor descriptor = (Descriptor) stateTypeUrl.getDescriptor();
            this.descriptor = checkNotNull(descriptor);
            return this;
        }

        /**
         * @param datastore the {@link DatastoreWrapper} to use in this storage
         */
        public Builder<I> setDatastore(DatastoreWrapper datastore) {
            this.datastore = checkNotNull(datastore);
            return this;
        }

        /**
         * @param multitenant {@code true} if the storage should be
         *                    {@link io.spine.server.storage.Storage#isMultitenant multitenant}
         *                    or not
         */
        public Builder<I> setMultitenant(boolean multitenant) {
            this.multitenant = multitenant;
            return this;
        }

        /**
         * @param columnTypeRegistry the type registry of the
         *                           {@linkplain io.spine.server.entity.storage.EntityColumn
         *                           entity columns}
         */
        public Builder<I> setColumnTypeRegistry(
                ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry) {
            this.columnTypeRegistry = checkNotNull(columnTypeRegistry);
            return this;
        }

        public Builder<I> setIdClass(Class<I> idClass) {
            this.idClass = checkNotNull(idClass);
            return this;
        }

        /**
         * Creates new instance of the {@code DsProjectionStorageDelegate}.
         */
        @SuppressWarnings("DuplicateStringLiteralInspection") // OK for the error messages.
        public DsProjectionStorageDelegate<I> build() {
            checkNotNull(descriptor, "State descriptor is not set.");
            checkNotNull(datastore, "Datastore is not set.");
            checkNotNull(columnTypeRegistry, "Column type registry is not set.");
            final DsProjectionStorageDelegate<I> storage =
                    new DsProjectionStorageDelegate<>(this);
            return storage;
        }
    }
}
