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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.base.EntityState;
import io.spine.logging.Logging;
import io.spine.server.BoundedContextBuilder;
import io.spine.server.ContextSpec;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.storage.ColumnMapping;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.Storage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.config.CreateEntityStorage;
import io.spine.server.storage.datastore.config.CreateMessageStorage;
import io.spine.server.storage.datastore.config.CreateStorage;
import io.spine.server.storage.datastore.config.CustomStorages;
import io.spine.server.storage.datastore.config.DsColumnMapping;
import io.spine.server.storage.datastore.config.RecordLayout;
import io.spine.server.storage.datastore.config.RecordLayouts;
import io.spine.server.storage.datastore.config.StorageConfiguration;
import io.spine.server.storage.datastore.config.TxSetting;
import io.spine.server.storage.datastore.config.TxSettings;
import io.spine.server.storage.datastore.record.DsEntitySpec;
import io.spine.server.storage.datastore.record.DsRecordStorage;
import io.spine.server.storage.datastore.tenant.DatastoreTenants;
import io.spine.server.storage.datastore.tenant.NamespaceConverter;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import io.spine.server.storage.datastore.tenant.NsConverterFactory;
import io.spine.server.storage.datastore.tenant.PrefixedNsConverterFactory;
import io.spine.server.tenant.TenantIndex;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Maps.newConcurrentMap;
import static io.spine.server.storage.datastore.DatastoreWrapper.wrap;
import static io.spine.server.storage.datastore.config.TxSetting.enabled;

/**
 * Creates {@link Storage}s based on {@link Datastore}.
 *
 * <p>As a convenience API, provides an ability to configure the {@link BoundedContextBuilder}s
 * with the {@link TenantIndex} specific to the instance of {@code Datastore} configured for this
 * factory
 *
 * @see DatastoreStorageFactory#configureTenantIndex(BoundedContextBuilder)
 */
public class DatastoreStorageFactory implements StorageFactory, Logging {

    private final Datastore datastore;

    /**
     * Cached instances of datastore wrappers per {@code ContextSpec}.
     *
     * <p>The repeated calls of the methods of this factory should refer to the same instance of
     * the wrapped {@code Datastore}. Then the storage configuration for the repositories
     * of the same {@code BoundedContext} is consistent.
     */
    private final Map<ContextSpec, DatastoreWrapper> contextWrappers = newConcurrentMap();

    /**
     * Cached instances of datastore wrappers initialized for system components, such as
     * a {@code DatastoreWrapper} used in the {@link io.spine.server.delivery.Delivery
     * Delivery}-specific {@link InboxStorage}.
     *
     * <p>The repeated calls of the methods of this factory should refer to the same instance of
     * the wrapped {@code Datastore} per class of the target {@code Storage}.
     */
    private final
    Map<Class<? extends Storage<?, ?>>, DatastoreWrapper> sysWrappers = newConcurrentMap();

    private final ColumnMapping<Value<?>> columnMapping;

    private final NsConverterFactory converterFactory;

    private final TxSettings txSettings;

    private final CustomStorages customStorages;

    private final RecordLayouts recordLayouts;

    protected DatastoreStorageFactory(Builder builder) {
        this.columnMapping = builder.columnMapping;
        this.datastore = builder.datastore;
        this.converterFactory = builder.converterFactory;
        this.txSettings = builder.txSettings.build();
        this.customStorages = builder.customStorages.build();
        this.recordLayouts = builder.layouts.build();
    }

    /**
     * Configures the passed {@link BoundedContextBuilder} with the {@link TenantIndex} built on
     * top of the {@code Datastore} specific to this factory instance.
     *
     * <p>This configuration is only suitable for multi-tenant {@code BoundedContext}s.
     *
     * @param builder
     *         the instance of the builder to configure the tenant index for
     * @return the same instance of the builder, but with the tenant index set
     */
    @CanIgnoreReturnValue
    public BoundedContextBuilder configureTenantIndex(BoundedContextBuilder builder) {
        checkNotNull(builder);
        TenantIndex index = DatastoreTenants.index(datastore, converterFactory());
        builder.setTenantIndex(index);
        return builder;
    }

    @Override
    public <I, R extends Message> RecordStorage<I, R>
    createRecordStorage(ContextSpec context, RecordSpec<I, R, ?> spec) {
        checkNotNull(context);
        checkNotNull(spec);
        StorageConfiguration<I, R> config = configurationWith(context, spec);
        Optional<CreateStorage<I, R>> custom = customStorages.find(spec);
        RecordStorage<I, R> result =
                custom.map(callback -> callback.apply(config))
                      .orElse(new DsRecordStorage<>(config));
        return result;
    }

    private <I, R extends Message>
    StorageConfiguration<I, R> configurationWith(ContextSpec context, RecordSpec<I, R, ?> spec) {
        DatastoreWrapper wrapper = wrapperFor(context);
        Class<? extends Message> recordType = spec.sourceType();
        TxSetting behavior = txSettings.find(recordType);
        RecordLayout<I, R> layout = recordLayouts.find(recordType);
        DsEntitySpec<I, R> dsSpec = new DsEntitySpec<>(spec, layout);
        StorageConfiguration<I, R> configuration = StorageConfiguration.<I, R>newBuilder()
                .withDatastore(wrapper)
                .withTxSetting(behavior)
                .withContext(context)
                .withMapping(columnMapping)
                .withRecordSpec(dsSpec)
                .build();
        return configuration;
    }

    public ColumnMapping<Value<?>> columnMapping() {
        return columnMapping;
    }

//    /**
//     * Configures the passed builder of the storage to serve the passed entity class.
//     */
//    private <I, R extends Message, S extends RecordStorage<I, R>,
//            B extends RecordStorageBuilder<I, S, B>>
//    S configure(B builder, Class<? extends Entity<I, ?>> cls, ContextSpec context) {
//        builder.setModelClass(asEntityClass(cls))
//               .setDatastore(wrapperFor(context))
//               .setMultitenant(context.isMultitenant())
//               .setColumnMapping(columnMapping);
//        S storage = builder.build();
//        return storage;
//    }

    private NamespaceSupplier createNamespaceSupplier(boolean multitenant) {
        String defaultNamespace = namespaceFromOptions();
        if (multitenant) {
            NsConverterFactory factory = converterFactory();
            return NamespaceSupplier.multitenant(factory);
        } else {
            return NamespaceSupplier.singleTenant(defaultNamespace);
        }
    }

    private NsConverterFactory converterFactory() {
        String defaultNamespace = namespaceFromOptions();
        return defaultNamespace.isEmpty()
               ? converterFactory
               : new PrefixedNsConverterFactory(defaultNamespace, converterFactory);
    }

    private String namespaceFromOptions() {
        return nullToEmpty(datastore.getOptions()
                                    .getNamespace());
    }

    /**
     * Performs no action.
     */
    @Override
    public void close() {
        // NOP
    }

    /**
     * Returns the currently known initialized {@code DatastoreWrapper}s.
     */
    @VisibleForTesting
    protected Iterable<DatastoreWrapper> wrappers() {
        return Iterables.concat(contextWrappers.values(), sysWrappers.values());
    }

    /**
     * Returns the instance of wrapped {@link Datastore}.
     */
    @VisibleForTesting
    protected Datastore datastore() {
        return datastore;
    }

    /**
     * Returns the instance of {@link DatastoreWrapper} based on the passed {@code ContextSpec}.
     *
     * <p>If there were no {@code DatastoreWrapper} instances created for the given context,
     * creates it.
     */
    final DatastoreWrapper wrapperFor(ContextSpec spec) {
        if (!contextWrappers.containsKey(spec)) {
            DatastoreWrapper wrapper = newDatastoreWrapper(spec.isMultitenant());
            contextWrappers.put(spec, wrapper);
        }
        return contextWrappers.get(spec);
    }

    /**
     * Creates a Datastore wrapper for system components,
     * such as {@link io.spine.server.delivery.Delivery Delivery}.
     *
     * @param targetStorage
     *         the storage to create a Datastore wrapper for
     * @param multitenant
     *         whether the wrapper should support multi-tenancy
     * @return a new instance of Datastore wrapper
     */
    @Internal
    public final DatastoreWrapper
    systemWrapperFor(Class<? extends Storage<?, ?>> targetStorage, boolean multitenant) {
        DatastoreWrapper wrapper = sysWrappers
                .computeIfAbsent(targetStorage, k -> newDatastoreWrapper(multitenant));
        return wrapper;
    }

    /**
     * Creates a new  instance of {@link DatastoreWrapper}.
     *
     * @param multitenant
     *         tells whether the created instance should support multi-tenancy
     */
    @Internal
    @VisibleForTesting
    protected DatastoreWrapper newDatastoreWrapper(boolean multitenant) {
        NamespaceSupplier supplier = createNamespaceSupplier(multitenant);
        return wrap(datastore, supplier);
    }

    /**
     * Creates new instance of {@code Builder}.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates a new instance of {@code Builder}, passing the {@code Datastore} to it, and
     * configuring the {@code Builder} instance with some default settings.
     */
    @VisibleForTesting
    public static Builder newBuilderWithDefaults(Datastore datastore) {
        checkNotNull(datastore);
        Builder result = newBuilder().setDatastore(datastore)
                                      .withDefaults();
        return result;
    }

    /**
     * A builder for the {@code DatastoreStorageFactory}.
     */
    public static class Builder {

        private Datastore datastore;
        private ColumnMapping<Value<?>> columnMapping;
        private NamespaceConverter namespaceConverter;
        private NsConverterFactory converterFactory;
        private final TxSettings.Builder txSettings = TxSettings.newBuilder();
        private final RecordLayouts.Builder layouts = RecordLayouts.newBuilder();
        private final CustomStorages.Builder customStorages = CustomStorages.newBuilder();

        /** Avoid direct initialization. */
        private Builder() {
        }

        /**
         * Assigns the {@link Datastore} to use for the DB interactions.
         *
         * <p>If the provided {@code Datastore} is configured with a namespace:
         * <ul>
         *     <li>resulting single tenant storages will use the provided namespace;
         *     <li>resulting multitenant storages will concatenate the provided namespace with
         *         the tenant identifier. See {@link #setNamespaceConverter} for more configuration.
         * </ul>
         */
        public Builder setDatastore(Datastore datastore) {
            this.datastore = checkNotNull(datastore);
            return this;
        }

        public Datastore getDatastore() {
            return this.datastore;
        }

        /**
         * Sets the {@link ColumnMapping} to use.
         *
         * <p>Default value is {@link DsColumnMapping}.
         *
         * @param columnMapping
         *         the storage rules for entity columns
         * @return self for method chaining
         */
        @CanIgnoreReturnValue
        public Builder setColumnMapping(ColumnMapping<Value<?>> columnMapping) {
            this.columnMapping = checkNotNull(columnMapping);
            return this;
        }

        /**
         * Sets a {@link NamespaceConverter} for converting the Datastore namespaces and
         * the {@link io.spine.core.TenantId Tenant IDs} back and forth.
         *
         * <p>Setting this parameter is reasonable (but not required) only if the storage is
         * multitenant. Otherwise, an exception will be thrown on {@linkplain #build() build}.
         *
         * @param converter
         *         a custom converter for the Tenant IDs
         * @return self for method chaining
         */
        @CanIgnoreReturnValue
        public Builder setNamespaceConverter(NamespaceConverter converter) {
            this.namespaceConverter = checkNotNull(converter);
            return this;
        }

        @CanIgnoreReturnValue
        public <R extends Message> Builder enableTransactions(Class<R> recordType) {
            checkNotNull(recordType);
            txSettings.add(recordType, enabled());
            return this;
        }

        @CanIgnoreReturnValue
        public <I, R extends Message>
        Builder useCustomStorage(Class<R> recordType, CreateMessageStorage<I, R> callback) {
            checkNotNull(recordType);
            checkNotNull(callback);
            customStorages.add(recordType, callback);
            return this;
        }

        @CanIgnoreReturnValue
        public <I, S extends EntityState<I>>
        Builder useCustomStorage(Class<S> stateType, CreateEntityStorage<I> callback) {
            checkNotNull(stateType);
            checkNotNull(callback);
            customStorages.add(stateType, callback);
            return this;
        }

        @CanIgnoreReturnValue
        public <I, R extends Message>
        Builder organizeRecords(Class<R> recordType, RecordLayout<I, R> layout) {
            checkNotNull(recordType);
            checkNotNull(layout);
            layouts.add(recordType, layout);
            return this;
        }

        /**
         * Creates a new instance of {@code DatastoreStorageFactory} with the passed parameters.
         *
         * <p>Precondition of a successful build is that the {@code datastore} field has been set.
         *
         * @return new instance of {@code DatastoreStorageFactory}
         */
        public DatastoreStorageFactory build() {
            checkNotNull(datastore);
            return new DatastoreStorageFactory(withDefaults());
        }

        @CanIgnoreReturnValue
        private Builder withDefaults() {
            setupMapping();
            setupNsConverter();
            return this;
        }

        private void setupNsConverter() {
            if (namespaceConverter == null) {
                converterFactory = NsConverterFactory.defaults();
            } else {
                converterFactory = multitenant -> namespaceConverter;
            }
        }

        private void setupMapping() {
            if (columnMapping == null) {
                columnMapping = new DsColumnMapping();
            }
        }
    }
}
