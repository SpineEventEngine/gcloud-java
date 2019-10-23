/*
 * Copyright 2019, TeamDev. All rights reserved.
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
import io.spine.annotation.Internal;
import io.spine.server.BoundedContextBuilder;
import io.spine.server.ContextSpec;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.entity.Entity;
import io.spine.server.entity.storage.ColumnStorageRules;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.Storage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.tenant.DatastoreTenants;
import io.spine.server.storage.datastore.tenant.NamespaceConverter;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import io.spine.server.storage.datastore.tenant.NsConverterFactory;
import io.spine.server.storage.datastore.tenant.PrefixedNsConverterFactory;
import io.spine.server.storage.datastore.type.DsStorageRules;
import io.spine.server.tenant.TenantIndex;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Maps.newConcurrentMap;
import static io.spine.server.entity.model.EntityClass.asEntityClass;
import static io.spine.server.storage.datastore.DatastoreWrapper.wrap;

/**
 * Creates {@link Storage}s based on {@link Datastore}.
 *
 * <p>As a convenience API, provides an ability to configure the {@link BoundedContextBuilder}s
 * with the {@link TenantIndex} specific to the instance of {@code Datastore} configured for this
 * factory
 *
 * @see DatastoreStorageFactory#configureTenantIndex(BoundedContextBuilder)
 */
public class DatastoreStorageFactory implements StorageFactory {

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
    private final Map<Class<? extends Storage>, DatastoreWrapper> sysWrappers = newConcurrentMap();

    private final ColumnStorageRules<Value<?>> columnStorageRules;

    private final NsConverterFactory converterFactory;

    protected DatastoreStorageFactory(Builder builder) {
        this.columnStorageRules = builder.columnStorageRules;
        this.datastore = builder.datastore;
        this.converterFactory = builder.converterFactory;
    }

    /**
     * Configures the passed {@link BoundedContextBuilder} with the {@link TenantIndex} built on
     * top of the {@code Datastore} specific to this factory instance.
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
    public <I> AggregateStorage<I>
    createAggregateStorage(ContextSpec context, Class<? extends Aggregate<I, ?, ?>> cls) {
        checkNotNull(cls);
        checkNotNull(context);

        DsAggregateStorage<I> result =
                new DsAggregateStorage<>(cls, wrapperFor(context), context.isMultitenant());
        return result;
    }

    @Override
    public <I> RecordStorage<I>
    createRecordStorage(ContextSpec context, Class<? extends Entity<I, ?>> cls) {
        checkNotNull(cls);
        checkNotNull(context);

        DsRecordStorage<I> result = configure(DsRecordStorage.newBuilder(), cls, context);
        return result;
    }

    @Override
    public <I> ProjectionStorage<I>
    createProjectionStorage(ContextSpec context, Class<? extends Projection<I, ?, ?>> cls) {
        checkNotNull(cls);
        checkNotNull(context);

        DsProjectionStorageDelegate<I> recordStorage =
                configure(DsProjectionStorageDelegate.newDelegateBuilder(), cls, context);
        DsPropertyStorage propertyStorage = createPropertyStorage(context);
        DsProjectionStorage<I> result =
                new DsProjectionStorage<>(cls,
                                          recordStorage,
                                          propertyStorage,
                                          context.isMultitenant());
        return result;
    }

    @Override
    public InboxStorage createInboxStorage(boolean multitenant) {
        DatastoreWrapper wrapper = systemWrapperFor(InboxStorage.class, multitenant);
        return new DsInboxStorage(wrapper, multitenant);
    }

    public ColumnStorageRules<Value<?>> columnStorageRules() {
        return columnStorageRules;
    }

    /**
     * Configures the passed builder of the storage to serve the passed entity class.
     */
    private <I, S extends RecordStorage<I>, B extends RecordStorageBuilder<I, S, B>>
    S configure(B builder, Class<? extends Entity<I, ?>> cls, ContextSpec context) {
        builder.setModelClass(asEntityClass(cls))
               .setDatastore(wrapperFor(context))
               .setMultitenant(context.isMultitenant())
               .setColumnStorageRules(columnStorageRules);
        S storage = builder.build();
        return storage;
    }

    protected DsPropertyStorage createPropertyStorage(ContextSpec spec) {
        DatastoreWrapper datastore = wrapperFor(spec);
        DsPropertyStorage propertyStorage = DsPropertyStorage.newInstance(datastore);
        return propertyStorage;
    }

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
        return nullToEmpty(datastore.getOptions().getNamespace());
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
            DatastoreWrapper wrapper = createDatastoreWrapper(spec.isMultitenant());
            contextWrappers.put(spec, wrapper);
        }
        return contextWrappers.get(spec);
    }

    final DatastoreWrapper systemWrapperFor(Class<? extends Storage> targetStorage,
                                            boolean multitenant) {
        DatastoreWrapper wrapper = sysWrappers
                .computeIfAbsent(targetStorage, k -> createDatastoreWrapper(multitenant));
        return wrapper;
    }

    /**
     * Creates an instance of {@link DatastoreWrapper} based on the passed {@code ContextSpec}.
     */
    @Internal
    @VisibleForTesting
    protected DatastoreWrapper createDatastoreWrapper(boolean multitenant) {
        NamespaceSupplier supplier = createNamespaceSupplier(multitenant);
        return wrap(datastore, supplier);
    }

    /**
     * Creates new instance of {@link Builder}.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for the {@code DatastoreStorageFactory}.
     */
    public static class Builder {

        private Datastore datastore;
        private ColumnStorageRules<Value<?>> columnStorageRules;
        private NamespaceConverter namespaceConverter;
        private NsConverterFactory converterFactory;

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
         * Sets the {@link ColumnStorageRules}.
         *
         * <p>Default value is {@link DsStorageRules}.
         *
         * @param columnStorageRules
         *         the storage rules for entity columns
         * @return self for method chaining
         */
        public Builder
        setColumnStorageRules(ColumnStorageRules<Value<?>> columnStorageRules) {
            this.columnStorageRules = checkNotNull(columnStorageRules);
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
        public Builder setNamespaceConverter(NamespaceConverter converter) {
            this.namespaceConverter = checkNotNull(converter);
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
            if (columnStorageRules == null) {
                columnStorageRules = new DsStorageRules();
            }
            if (namespaceConverter == null) {
                converterFactory = NsConverterFactory.defaults();
            } else {
                converterFactory = multitenant -> namespaceConverter;
            }
            return new DatastoreStorageFactory(this);
        }
    }
}
