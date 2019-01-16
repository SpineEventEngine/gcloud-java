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
import com.google.cloud.datastore.DatastoreOptions;
import io.spine.annotation.Internal;
import io.spine.core.BoundedContextName;
import io.spine.server.BoundedContext;
import io.spine.server.BoundedContextBuilder;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.entity.Entity;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.tenant.DatastoreTenants;
import io.spine.server.storage.datastore.tenant.NamespaceConverter;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import io.spine.server.storage.datastore.tenant.TenantConverterRegistry;
import io.spine.server.storage.datastore.type.DatastoreColumnType;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import io.spine.server.tenant.TenantIndex;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.spine.server.entity.model.EntityClass.asEntityClass;
import static io.spine.server.storage.datastore.DatastoreWrapper.wrap;

/**
 * Creates storages based on {@link Datastore}.
 *
 * @see DatastoreStorageFactory#newBoundedContextBuilder for the recommended usage description
 */
public class DatastoreStorageFactory implements StorageFactory {

    private final DatastoreWrapper datastore;
    private final boolean multitenant;
    private final ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> typeRegistry;
    private final @MonotonicNonNull NamespaceConverter namespaceConverter;

    @SuppressWarnings({
            /* Overridden method required for stub. impl. of test environments.  */
            "OverridableMethodCallDuringObjectConstruction",
            "OverriddenMethodCallDuringObjectConstruction"
    })
    DatastoreStorageFactory(Builder builder) {
        this.multitenant = builder.multitenant;
        this.typeRegistry = builder.typeRegistry;
        this.namespaceConverter = builder.namespaceConverter;
        this.datastore = createDatastoreWrapper(builder);
    }

    /**
     * Creates new instance of the {@link BoundedContextBuilder}.
     *
     * <p>The returned instance has the following attributes pre-configured:
     * <ul>
     *     <li>{@link BoundedContextBuilder#setStorageFactorySupplier(Supplier) Supplier}
     *     of {@code StorageFactory};
     *     <li>{@link BoundedContextBuilder#setTenantIndex(TenantIndex) TenantIndex};
     *     <li>{@linkplain BoundedContextBuilder#setMultitenant(boolean) multitenancy flag}.
     * </ul>
     *
     * <p>In a majority of use cases the configuration of the produced
     * {@link BoundedContextBuilder builder} is enough for operation. However, it is still possible
     * to use the returned instance for further customization.
     *
     * @return new instance of {@link BoundedContextBuilder BoundedContextBuilder}
     *         with the specified parameters
     */
    public BoundedContextBuilder newBoundedContextBuilder() {
        Datastore datastore = getDatastore()
                .getDatastoreOptions()
                .getService();
        TenantIndex tenantIndex = DatastoreTenants.index(datastore);
        Supplier<StorageFactory> storageFactorySupplier = () -> this;
        BoundedContextBuilder resultBuilder =
                BoundedContext.newBuilder()
                              .setMultitenant(isMultitenant())
                              .setStorageFactorySupplier(storageFactorySupplier)
                              .setTenantIndex(tenantIndex);
        return resultBuilder;
    }

    private Builder toBuilder() {
        Builder result = newBuilder()
                .setDatastore(datastore.getDatastore())
                .setMultitenant(multitenant);
        if (!multitenant) {
            result.setNamespaceConverter(namespaceConverter);
        }

        return result;
    }

    protected DatastoreWrapper createDatastoreWrapper(Builder builder) {
        DatastoreWrapper wrapped = wrap(builder.datastore, builder.namespaceSupplier);
        return wrapped;
    }

    @Override
    public boolean isMultitenant() {
        return multitenant;
    }

    @Override
    public ColumnTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    @Override
    public <I>
    ProjectionStorage<I> createProjectionStorage(Class<? extends Projection<I, ?, ?>> cls) {
        DsProjectionStorageDelegate<I> recordStorage =
                configure(DsProjectionStorageDelegate.newDelegateBuilder(), cls).build();
        DsPropertyStorage propertyStorage = createPropertyStorage();
        DsProjectionStorage<I> result =
                new DsProjectionStorage<>(cls, recordStorage, propertyStorage, multitenant);
        return result;
    }

    @Override
    public <I> RecordStorage<I> createRecordStorage(Class<? extends Entity<I, ?>> cls) {
        DsRecordStorage<I> result = configure(DsRecordStorage.newBuilder(), cls).build();
        return result;
    }

    /**
     * Configures the passed builder of the storage to serve the passed entity class.
     */
    private <I, B extends RecordStorageBuilder<I, B>>
    B configure(B builder, Class<? extends Entity<I, ?>> cls) {
        builder.setModelClass(asEntityClass(cls))
               .setDatastore(getDatastore())
               .setMultitenant(isMultitenant())
               .setColumnTypeRegistry(typeRegistry);
        return builder;
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(Class<? extends Aggregate<I, ?, ?>> cls) {
        checkNotNull(cls);
        DsPropertyStorage propertyStorage = createPropertyStorage();
        DsAggregateStorage<I> result =
                new DsAggregateStorage<>(cls, getDatastore(), propertyStorage, multitenant);
        return result;
    }

    @Override
    public StorageFactory toSingleTenant() {
        return isMultitenant()
               ? copyFor(BoundedContextName.getDefaultInstance(), false)
               : this;
    }

    @Override
    public StorageFactory copyFor(BoundedContextName name, boolean multitenant) {
        return toBuilder().setMultitenant(multitenant)
                          .build();
    }

    protected DsPropertyStorage createPropertyStorage() {
        DsPropertyStorage propertyStorage = DsPropertyStorage.newInstance(getDatastore());
        return propertyStorage;
    }

    /**
     * Performs no action.
     */
    @Override
    public void close() {
        // NOP
    }

    /**
     * Obtains an instance of a wrapper of the passed {@link Datastore}.
     */
    @Internal
    public DatastoreWrapper getDatastore() {
        return datastore;
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

        private static final String DEFAULT_NAMESPACE_ERROR_MESSAGE =
                "Datastore namespace should not be configured explicitly" +
                        "for a multitenant storage";
        private static final String REDUNDANT_TENANT_ID_CONVERTER_ERROR_MESSAGE =
                "Setting a custom NamespaceToTenantIdConverter is not allowed" +
                        " for a single-tenant storage factory.";

        private Datastore datastore;
        private boolean multitenant;
        private ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> typeRegistry;
        private NamespaceSupplier namespaceSupplier;
        private NamespaceConverter namespaceConverter;

        /** Avoid direct initialization. */
        private Builder() {
        }

        /**
         * Assigns the {@link Datastore} to use for the DB interactions.
         */
        public Builder setDatastore(Datastore datastore) {
            this.datastore = checkNotNull(datastore);
            return this;
        }

        public Datastore getDatastore() {
            return this.datastore;
        }

        /**
         * Enables the multitenancy support for the {@code DatastoreStorageFactory}.
         *
         * <p>By default this option is {@code false}.
         *
         * <p>If the multitenancy is enabled, the passed {@link Datastore} should not have
         * a {@code namespace} set explicitly.
         *
         * @param multitenant {@code true} if the {@code DatastoreStorageFactory} should
         *                    be multitenant or not
         * @return self for method chaining
         * @see io.spine.server.storage.datastore.tenant.DatastoreTenants
         */
        public Builder setMultitenant(boolean multitenant) {
            this.multitenant = multitenant;
            return this;
        }

        /**
         * Sets a {@link ColumnTypeRegistry} for handling the Entity Columns.
         *
         * <p>Default value is {@link DatastoreTypeRegistryFactory#defaultInstance()}.
         *
         * @param typeRegistry the type registry containing all the required
         *                     {@linkplain io.spine.server.entity.storage.ColumnType column types}
         *                     to handle the existing Entity Columns
         * @return self for method chaining
         */
        public Builder
        setTypeRegistry(ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> typeRegistry) {
            this.typeRegistry = checkNotNull(typeRegistry);
            return this;
        }

        /**
         * Sets a {@link NamespaceConverter} for converting the Datastore namespaces and
         * the {@link io.spine.core.TenantId Tenant IDs} back and forth.
         *
         * <p>Setting this parameter is reasonable (but not required) only if the storage is
         * multitenant. Otherwise, an exception will be thrown on {@linkplain #build() build}.
         *
         * @param converter a custom converter for the Tenant IDs
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
            this.namespaceSupplier = createNamespaceSupplier();
            if (typeRegistry == null) {
                typeRegistry = DatastoreTypeRegistryFactory.defaultInstance();
            }
            if (!multitenant) {
                checkArgument(namespaceConverter == null,
                              REDUNDANT_TENANT_ID_CONVERTER_ERROR_MESSAGE);
            }
            if (namespaceConverter != null) {
                ProjectId projectId = ProjectId.of(datastore);
                TenantConverterRegistry.registerNamespaceConverter(projectId, namespaceConverter);
            }

            return new DatastoreStorageFactory(this);
        }

        private NamespaceSupplier createNamespaceSupplier() {
            @Nullable String defaultNamespace;
            if (multitenant) {
                checkHasNoNamespace(datastore);
                defaultNamespace = null;
            } else {
                defaultNamespace = datastore.getOptions()
                                            .getNamespace();
            }
            ProjectId projectId = ProjectId.of(datastore);
            NamespaceSupplier result =
                    NamespaceSupplier.instance(multitenant, defaultNamespace, projectId);
            return result;
        }

        private static void checkHasNoNamespace(Datastore datastore) {
            checkNotNull(datastore);
            DatastoreOptions options = datastore.getOptions();
            String namespace = options.getNamespace();
            checkArgument(isNullOrEmpty(namespace), DEFAULT_NAMESPACE_ERROR_MESSAGE);
        }
    }
}
