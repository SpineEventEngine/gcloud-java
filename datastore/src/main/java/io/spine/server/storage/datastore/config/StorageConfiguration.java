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

package io.spine.server.storage.datastore.config;

import com.google.cloud.datastore.Value;
import com.google.protobuf.Message;
import io.spine.server.ContextSpec;
import io.spine.server.storage.ColumnMapping;
import io.spine.server.storage.datastore.DatastoreWrapper;
import io.spine.server.storage.datastore.record.DsEntitySpec;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A configuration of a Datastore-backed record storage.
 *
 * @param <I>
 *         the type of identifiers of the records persisted in the configured storage
 * @param <R>
 *         the type of records persisted in the configured storage
 */
public final class StorageConfiguration<I, R extends Message> {

    private final ContextSpec context;
    private final DatastoreWrapper datastore;
    private final DsEntitySpec<I, R> recordSpec;
    private final ColumnMapping<Value<?>> columnMapping;
    private final TxSetting txSetting;

    private StorageConfiguration(Builder<I, R> builder) {
        this.context = builder.context;
        this.datastore = builder.datastore;
        this.recordSpec = builder.recordSpec;
        this.columnMapping = builder.columnMapping;
        this.txSetting = builder.txSetting;
    }

    /**
     * Returns the specification of the Bounded Context, in scope of which the configured storage
     * is used.
     */
    public ContextSpec context() {
        return context;
    }

    /**
     * Returns the Datastore wrapper configured for the storage.
     */
    public DatastoreWrapper datastore() {
        return datastore;
    }

    /**
     * Returns the record specification to use in the configured storage.
     */
    public DsEntitySpec<I, R> recordSpec() {
        return recordSpec;
    }

    /**
     * Returns the type of records stored by the configured storage.
     */
    public Class<R> storedType() {
        return recordSpec.recordSpec()
                         .storedType();
    }

    /**
     * Returns the column mapping to apply for the configured storage.
     */
    public ColumnMapping<Value<?>> columnMapping() {
        return columnMapping;
    }

    /**
     * Returns the transactional setting for the configured storage.
     */
    public TxSetting txSetting() {
        return txSetting;
    }

    /**
     * Creates a new builder of {@code StorageConfiguration} instances.
     *
     * @param <I>
     *         the type of identifiers of the records persisted in the configured storage
     * @param <R>
     *         the type of records persisted in the configured storage
     * @return a new builder instance
     */
    public static <I, R extends Message> Builder<I, R> newBuilder() {
        return new Builder<>();
    }

    /**
     * A builder of {@code StorageConfiguration} instances.
     *
     * @param <I>
     *         the type of identifiers of the records persisted in the configured storage
     * @param <R>
     *         the type of records persisted in the configured storage
     */
    public static class Builder<I, R extends Message> {

        private ContextSpec context;
        private DatastoreWrapper datastore;
        private DsEntitySpec<I, R> recordSpec;
        private ColumnMapping<Value<?>> columnMapping;
        private TxSetting txSetting;

        /**
         * Prevents this builder from direct instantiation.
         */
        private Builder() {
        }

        /**
         * Sets the specification of the Bounded Context in scope of which the configured storage
         * will be used.
         *
         * <p>This parameter is mandatory.
         *
         * <p>Overrides the previous value, if set.
         *
         * @param context
         *         the specification of the Bounded Context
         * @return this instance of this {@code Builder}, for call chaining
         */
        public Builder<I, R> withContext(ContextSpec context) {
            this.context = checkNotNull(context);
            return this;
        }

        /**
         * Sets the Datastore wrapper to use by the storage.
         *
         * <p>This parameter is mandatory.
         *
         * <p>Overrides the previous value, if set.
         *
         * @param datastore
         *         the Datastore wrapper
         * @return this instance of this {@code Builder}, for call chaining
         */
        public Builder<I, R> withDatastore(DatastoreWrapper datastore) {
            this.datastore = checkNotNull(datastore);
            return this;
        }

        /**
         * Sets the record specification for the configured storage.
         *
         * <p>This parameter is mandatory.
         *
         * <p>Overrides the previous value, if set.
         *
         * @param recordSpec
         *         the record specification
         * @return this instance of this {@code Builder}, for call chaining
         */
        public Builder<I, R> withRecordSpec(DsEntitySpec<I, R> recordSpec) {
            this.recordSpec = checkNotNull(recordSpec);
            return this;
        }

        /**
         * Sets the column type mapping for the configured storage
         *
         * <p>This parameter is mandatory.
         *
         * <p>Overrides the previous value, if set.
         *
         * @param columnMapping
         *         the column type mapping
         * @return this instance of this {@code Builder}, for call chaining
         */
        public Builder<I, R> withMapping(ColumnMapping<Value<?>> columnMapping) {
            this.columnMapping = checkNotNull(columnMapping);
            return this;
        }

        /**
         * Sets the transactional setting for the configured storage.
         *
         * <p>This parameter is optional. By default, transactional behavior is set
         * to {@linkplain TxSetting#disabled() disabled}.
         *
         * <p>Overrides the previous value, if set.
         *
         * @param txSetting
         *         the setting of transactional behavior
         * @return this instance of this {@code Builder}, for call chaining
         */
        public Builder<I, R> withTxSetting(TxSetting txSetting) {
            this.txSetting = checkNotNull(txSetting);
            return this;
        }

        /**
         * Builds a new {@code StorageConfiguration} on top of this {@code Builder}.
         *
         * <p>The required fields are as follows:
         * <ul>
         *     <li>{@linkplain #withContext(ContextSpec) specification of the Bounded Context},
         *     <li>{@linkplain #withDatastore(DatastoreWrapper) Datastore wrapper},
         *     <li>{@linkplain #withRecordSpec(DsEntitySpec) specification of the stored record},
         *     <li>{@linkplain #withMapping(ColumnMapping) column mapping}.
         * </ul>
         */
        public StorageConfiguration<I, R> build() {
            ensureSet(context);
            ensureSet(datastore);
            ensureSet(recordSpec);
            ensureSet(columnMapping);

            configureTxSetting();
            return new StorageConfiguration<>(this);
        }

        private static void ensureSet(Object setting) {
            checkNotNull(setting, "`%s` must be set.",
                         setting.getClass()
                                .getSimpleName());
        }

        private void configureTxSetting() {
            if (txSetting == null) {
                txSetting = TxSetting.disabled();
            }
        }
    }
}
