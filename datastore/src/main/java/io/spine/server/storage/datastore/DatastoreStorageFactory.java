/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import io.spine.logging.WithLogging;
import io.spine.server.BoundedContextBuilder;
import io.spine.server.ContextSpec;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.storage.ColumnMapping;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.Storage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.config.CreateEntityStorage;
import io.spine.server.storage.datastore.config.CreateRecordStorage;
import io.spine.server.storage.datastore.config.CustomStorages;
import io.spine.server.storage.datastore.config.DsColumnMapping;
import io.spine.server.storage.datastore.config.RecordLayout;
import io.spine.server.storage.datastore.config.RecordLayouts;
import io.spine.server.storage.datastore.config.StorageConfiguration;
import io.spine.server.storage.datastore.config.TxSettings;
import io.spine.server.storage.datastore.record.DsEntitySpec;
import io.spine.server.storage.datastore.record.DsRecordStorage;
import io.spine.server.storage.datastore.tenant.DatastoreTenants;
import io.spine.server.storage.datastore.tenant.NamespaceConverter;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import io.spine.server.storage.datastore.tenant.NamespaceConverterFactory;
import io.spine.server.storage.datastore.tenant.PrefixedNsConverterFactory;
import io.spine.server.tenant.TenantIndex;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Maps.newConcurrentMap;
import static io.spine.server.storage.datastore.DatastoreWrapper.wrap;
import static io.spine.server.storage.datastore.config.TxSetting.enabled;

/**
 * Creates {@link Storage}s based on {@link Datastore}.
 *
 * <p>As a convenience API, provides an ability to configure the {@link BoundedContextBuilder}s
 * with the {@link TenantIndex} specific to the instance of {@code Datastore} configured for this
 * factory.
 *
 * <p>As per design intention of {@link StorageFactory}, by default all storages created by
 * this factory delegate the execution to instances of a pre-configured {@link DsRecordStorage}.
 *
 * @see DatastoreStorageFactory#configureTenantIndex(BoundedContextBuilder)
 */
public class DatastoreStorageFactory implements StorageFactory, WithLogging {

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

    /**
     * The mapping of the values from the Java type system to the types native to Datastore.
     */
    private final ColumnMapping<Value<?>> columnMapping;

    /**
     * A factory of {@link io.spine.server.storage.datastore.tenant.Namespace} converters.
     */
    private final NamespaceConverterFactory converterFactory;

    /**
     * The settings of transactional behavior, per each stored record type.
     */
    private final TxSettings txSettings;

    /**
     * The set of functions producing custom storage implementations, if set by library end-users.
     */
    private final CustomStorages customStorages;

    /**
     * Layouts of records stored as Datastore Entities, per stored record type.
     */
    private final RecordLayouts recordLayouts;

    protected DatastoreStorageFactory(Builder builder) {
        this.columnMapping = builder.columnMapping;
        this.datastore = builder.datastore;
        this.converterFactory = builder.effectiveConverterFactory();
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
        var index = DatastoreTenants.index(datastore, converterFactory());
        builder.setTenantIndex(index);
        return builder;
    }

    @Override
    public <I, R extends Message> RecordStorage<I, R>
    createRecordStorage(ContextSpec context, RecordSpec<I, R> spec) {
        checkNotNull(context);
        checkNotNull(spec);
        var config = configurationWith(context, spec);
        var custom = customStorages.find(spec);
        var result =
                custom.map(callback -> callback.apply(config))
                      .orElse(new DsRecordStorage<>(config));
        return result;
    }

    private <I, R extends Message>
    StorageConfiguration<I, R> configurationWith(ContextSpec context, RecordSpec<I, R> spec) {
        var wrapper = wrapperFor(context);
        var recordType = spec.sourceType();
        var behavior = txSettings.find(recordType);
        RecordLayout<I, R> layout = recordLayouts.find(recordType);
        var dsSpec = new DsEntitySpec<>(spec, layout);
        var configuration = StorageConfiguration.<I, R>newBuilder()
                .withDatastore(wrapper)
                .withTxSetting(behavior)
                .withContext(context)
                .withMapping(columnMapping)
                .withRecordSpec(dsSpec)
                .build();
        return configuration;
    }

    /**
     * Returns the column mapping set for this factory.
     */
    public final ColumnMapping<Value<?>> columnMapping() {
        return columnMapping;
    }

    /**
     * Returns the {@link NamespaceConverterFactory} configured for this factory.
     */
    @VisibleForTesting
    NamespaceConverterFactory namespaceConverterFactory() {
        return converterFactory;
    }

    private NamespaceSupplier createNamespaceSupplier(boolean multitenant) {
        var defaultNamespace = namespaceFromOptions();
        if (multitenant) {
            var factory = converterFactory();
            return NamespaceSupplier.multitenant(factory);
        } else {
            return NamespaceSupplier.singleTenant(defaultNamespace);
        }
    }

    private NamespaceConverterFactory converterFactory() {
        var defaultNamespace = namespaceFromOptions();
        return defaultNamespace.isEmpty()
               ? converterFactory
               : new PrefixedNsConverterFactory(defaultNamespace, converterFactory);
    }

    private String namespaceFromOptions() {
        return nullToEmpty(datastore.getOptions()
                                    .getNamespace());
    }

    /**
     * Always returns {@code true}.
     *
     * <p>This factory holds no closeable resources of its own, and its {@link #close()}
     * performs no action, so the factory is always considered open.
     */
    @Override
    public boolean isOpen() {
        return true;
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
            var wrapper = newDatastoreWrapper(spec.isMultitenant());
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
        var wrapper = sysWrappers.computeIfAbsent(targetStorage, k -> newDatastoreWrapper(multitenant));
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
        var supplier = createNamespaceSupplier(multitenant);
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
     * configuring the {@code Builder} instance with some default settings, such as
     * {@linkplain DsColumnMapping column mapping} and
     * {@linkplain NamespaceConverterFactory#defaults() namespace converter factory}.
     */
    @VisibleForTesting
    public static Builder newBuilderWithDefaults(Datastore datastore) {
        checkNotNull(datastore);
        var result = newBuilder()
                .setDatastore(datastore)
                .withDefaults();
        return result;
    }

    /**
     * A builder for the {@code DatastoreStorageFactory}.
     */
    public static class Builder {

        private static final String CONVERTER_OPTIONS_CONFLICT =
                "`setNamespaceConverter` and `setConverterFactory` are mutually exclusive; " +
                        "only one of them may be called.";

        private Datastore datastore;
        private ColumnMapping<Value<?>> columnMapping;
        private NamespaceConverter namespaceConverter;
        private NamespaceConverterFactory converterFactory;
        private final TxSettings.Builder txSettings = TxSettings.newBuilder();
        private final RecordLayouts.Builder layouts = RecordLayouts.newBuilder();
        private final CustomStorages.Builder customStorages = CustomStorages.newBuilder();

        /** Avoid direct initialization. */
        private Builder() {
        }

        /**
         * Assigns the {@link Datastore} to use for the storage interactions.
         *
         * <p>If the provided {@code Datastore} is configured with a namespace:
         * <ul>
         *     <li>resulting single tenant storages will use the provided namespace;
         *     <li>resulting multitenant storages will concatenate the provided namespace with
         *         the tenant identifier. See {@link #setNamespaceConverter} for more configuration.
         * </ul>
         *
         * @return this instance of {@code Builder}
         */
        public Builder setDatastore(Datastore datastore) {
            this.datastore = checkNotNull(datastore);
            return this;
        }

        /**
         * Sets the {@link ColumnMapping} to use.
         *
         * <p>Default value is {@link DsColumnMapping}.
         *
         * @param columnMapping
         *         the storage rules for entity columns
         * @return this instance of {@code Builder}
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
         * multitenant. Otherwise, the converter is not consulted.
         *
         * <p>This is a shorthand for supplying an {@link NamespaceConverterFactory} that returns the
         * same converter regardless of the multi-tenancy setting. To vary the converter by
         * multi-tenancy, use {@link #setConverterFactory(NamespaceConverterFactory)} instead. The two
         * methods are mutually exclusive.
         *
         * @param converter
         *         a custom converter for the Tenant IDs
         * @return this instance of {@code Builder}
         * @throws IllegalStateException
         *         if an {@link NamespaceConverterFactory} has already been set via
         *         {@link #setConverterFactory(NamespaceConverterFactory)}
         */
        @CanIgnoreReturnValue
        public Builder setNamespaceConverter(NamespaceConverter converter) {
            checkNotNull(converter);
            checkState(converterFactory == null, CONVERTER_OPTIONS_CONFLICT);
            this.namespaceConverter = converter;
            return this;
        }

        /**
         * Sets an {@link NamespaceConverterFactory} producing the {@link NamespaceConverter}s used to
         * convert the Datastore namespaces and the {@link io.spine.core.TenantId Tenant IDs}
         * back and forth.
         *
         * <p>Unlike {@link #setNamespaceConverter(NamespaceConverter)}, the factory is given the
         * multi-tenancy setting of the storage and may produce a different converter for
         * single-tenant and multi-tenant environments. The produced converter is consulted only
         * for multitenant storages.
         *
         * <p>If neither this method nor {@link #setNamespaceConverter(NamespaceConverter)} is
         * called, the {@linkplain NamespaceConverterFactory#defaults() default} factory is used.
         *
         * @param converterFactory
         *         a custom factory of the Tenant ID converters
         * @return this instance of {@code Builder}
         * @throws IllegalStateException
         *         if a {@link NamespaceConverter} has already been set via
         *         {@link #setNamespaceConverter(NamespaceConverter)}
         */
        @CanIgnoreReturnValue
        public Builder setConverterFactory(NamespaceConverterFactory converterFactory) {
            checkNotNull(converterFactory);
            checkState(namespaceConverter == null, CONVERTER_OPTIONS_CONFLICT);
            this.converterFactory = converterFactory;
            return this;
        }

        /**
         * Enables the transactional operations for the given type of stored records.
         *
         * @param recordType
         *         the stored type
         * @param <R>
         *         the stored type
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <R extends Message> Builder enableTransactions(Class<R> recordType) {
            checkNotNull(recordType);
            txSettings.add(recordType, enabled());
            return this;
        }

        /**
         * Tells to use a custom function to create a record storage when this factory is
         * asked to provide a storage for a specified record type.
         *
         * <p>If the record type is an {@link io.spine.server.entity.Entity Entity} state,
         * please use {@link #useEntityStorage(Class, CreateEntityStorage)
         * useEntityStorage(entityStateType, CreateEntityStorage)}.
         *
         * @param id
         *         the type of identifiers of the stored records
         * @param record
         *         the stored type
         * @param callback
         *         a callback to create a custom storage
         * @param <I>
         *         the type of identifiers of stored records
         * @param <R>
         *         the stored type
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        @SuppressWarnings("unused") /* `id` parameter used to set `I` value explicitly. */
        public <I, R extends Message>
        Builder useRecordStorage(Class<I> id, Class<R> record, CreateRecordStorage<I, R> callback) {
            checkNotNull(id);
            checkNotNull(record);
            checkNotNull(callback);
            customStorages.add(record, callback);
            return this;
        }

        /**
         * Tells to use a custom function to create a storage of Spine Entities when this factory is
         * asked to provide a storage for a specified entity type.
         *
         * <p>If the record type is not an {@link io.spine.server.entity.Entity Entity} state,
         * please use {@link #useRecordStorage(Class, Class, CreateRecordStorage)
         * useRecordStorage(idType, recordType, CreateRecordStorage)}.
         *
         * @param stateType
         *         the type of the stored Spine's Entity state
         * @param callback
         *         a callback to create a custom storage
         * @param <I>
         *         the type of identifiers of stored records
         * @param <S>
         *         the type of the stored Spine's Entity state
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <I, S extends EntityState<I>>
        Builder useEntityStorage(Class<S> stateType, CreateEntityStorage<I> callback) {
            checkNotNull(stateType);
            checkNotNull(callback);
            customStorages.add(stateType, callback);
            return this;
        }

        /**
         * Specified the layout of Datastore Entities to use when operating with the records of
         * a particular type.
         *
         * @param recordType
         *         the type of stored records
         * @param layout
         *         the layout to use
         * @param <I>
         *         the type of record identifiers
         * @param <R>
         *         the type of stored records
         * @return this instance of {@code Builder}
         */
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
            return this;
        }

        /**
         * Returns the {@link NamespaceConverterFactory} to use, derived from the configured options.
         *
         * <p>A {@linkplain #setConverterFactory(NamespaceConverterFactory) factory set directly} takes
         * precedence; otherwise a {@linkplain #setNamespaceConverter(NamespaceConverter) namespace
         * converter}, if set, is wrapped into a factory; otherwise the
         * {@linkplain NamespaceConverterFactory#defaults() default} factory is used.
         */
        private NamespaceConverterFactory effectiveConverterFactory() {
            if (converterFactory != null) {
                return converterFactory;
            }
            if (namespaceConverter != null) {
                return multitenant -> namespaceConverter;
            }
            return NamespaceConverterFactory.defaults();
        }

        private void setupMapping() {
            if (columnMapping == null) {
                columnMapping = new DsColumnMapping();
            }
        }
    }
}
