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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.entity.Entity;
import io.spine.server.entity.model.EntityClass;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.datastore.tenant.NamespaceSupplier;
import io.spine.server.storage.datastore.tenant.NamespaceToTenantIdConverter;
import io.spine.server.storage.datastore.tenant.TenantConverterRegistry;
import io.spine.server.storage.datastore.type.DatastoreColumnType;
import io.spine.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.spine.server.entity.model.EntityClass.asEntityClass;

/**
 * Creates storages based on GAE {@link Datastore}.
 *
 * @author Alexander Litus
 * @author Mikhail Mikhaylov
 * @author Dmytro Dashenkov
 * @see io.spine.server.datastore.Contexts#onTopOf for the recommended usage description
 */
@SuppressWarnings("WeakerAccess") // Part of API
public class DatastoreStorageFactory implements StorageFactory {

    private final DatastoreWrapper datastore;
    private final boolean multitenant;
    private final ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> typeRegistry;
    private final NamespaceSupplier namespaceSupplier;
    private final NamespaceToTenantIdConverter namespaceToTenantIdConverter;

    private DatastoreStorageFactory(Builder builder) {
        this(builder.datastore,
             builder.multitenant,
             builder.typeRegistry,
             builder.namespaceSupplier,
             builder.namespaceToTenantIdConverter);
    }

    @VisibleForTesting
    @SuppressWarnings({"OverridableMethodCallDuringObjectConstruction", "OverriddenMethodCallDuringObjectConstruction"})
    protected DatastoreStorageFactory(Datastore datastore,
                                      boolean multitenant,
                                      ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> typeRegistry,
                                      NamespaceSupplier namespaceSupplier,
                                      @Nullable NamespaceToTenantIdConverter namespaceConverter) {
        this.multitenant = multitenant;
        this.typeRegistry = typeRegistry;
        this.namespaceSupplier = namespaceSupplier;
        this.datastore = createDatastoreWrapper(datastore);
        this.namespaceToTenantIdConverter = namespaceConverter;
    }

    protected DatastoreStorageFactory(DatastoreWrapper datastore,
                                      boolean multitenant,
                                      ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> typeRegistry,
                                      NamespaceSupplier namespaceSupplier,
                                      @Nullable NamespaceToTenantIdConverter namespaceConverter) {
        this.datastore = checkNotNull(datastore);
        this.multitenant = multitenant;
        this.typeRegistry = typeRegistry;
        this.namespaceSupplier = namespaceSupplier;
        this.namespaceToTenantIdConverter = namespaceConverter;
    }

    protected DatastoreWrapper createDatastoreWrapper(Datastore datastore) {
        checkState(this.getDatastore() == null, "Datastore is already initialized");
        DatastoreWrapper wrapped = DatastoreWrapper.wrap(datastore, namespaceSupplier);
        return wrapped;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMultitenant() {
        return multitenant;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ColumnTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StorageFactory toSingleTenant() {
        return isMultitenant()
               ? new DatastoreStorageFactory(getDatastore(),
                                             false,
                                             typeRegistry,
                                             NamespaceSupplier.singleTenant(),
                                             namespaceToTenantIdConverter)
               : this;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked") // The ID class is ensured by the parameter type.
    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(
            Class<? extends Projection<I, ?, ?>> projectionClass) {
        EntityClass<? extends Projection<I, ?, ?>> entityClass = asEntityClass(projectionClass);
        TypeUrl stateType = entityClass.getStateType();
        Class<I> idClass = (Class<I>) entityClass.getIdClass();
        DsProjectionStorageDelegate<I> recordStorage =
                DsProjectionStorageDelegate.<I>newDelegateBuilder()
                                           .setDatastore(getDatastore())
                                           .setMultitenant(isMultitenant())
                                           .setIdClass(idClass)
                                           .setEntityClass(projectionClass)
                                           .setStateType(stateType)
                                           .setColumnTypeRegistry(typeRegistry)
                                           .build();
        DsPropertyStorage propertyStorage = createPropertyStorage();
        DsProjectionStorage<I> result = new DsProjectionStorage<>(recordStorage,
                                                                        propertyStorage,
                                                                        projectionClass,
                                                                        multitenant);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked") // The ID class is ensured by the parameter type.
    @Override
    public <I> RecordStorage<I> createRecordStorage(Class<? extends Entity<I, ?>> entityClass) {
        EntityClass<? extends Entity<I, ?>> wrappedEntityClass = asEntityClass(entityClass);
        TypeUrl stateType = wrappedEntityClass.getStateType();
        Class<I> idClass = (Class<I>) wrappedEntityClass.getIdClass();
        DsRecordStorage<I> result = DsRecordStorage.<I>newBuilder()
                                                         .setStateType(stateType)
                                                         .setDatastore(getDatastore())
                                                         .setMultitenant(isMultitenant())
                                                         .setColumnTypeRegistry(typeRegistry)
                                                         .setIdClass(idClass)
                                                         .setEntityClass(entityClass)
                                                         .build();
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked") // The ID class is ensured by the parameter type.
    @Override
    public <I> AggregateStorage<I> createAggregateStorage(
            Class<? extends Aggregate<I, ?, ?>> entityClass) {
        checkNotNull(entityClass);
        EntityClass<? extends Aggregate<I, ?, ?>> wrappedEntityClass = asEntityClass(entityClass);
        DsPropertyStorage propertyStorage = createPropertyStorage();
        Class<I> idClass = (Class<I>) wrappedEntityClass.getIdClass();
        Class<? extends Message> stateClass = wrappedEntityClass.getStateClass();
        DsAggregateStorage<I> result = new DsAggregateStorage<>(getDatastore(),
                                                                      propertyStorage,
                                                                      multitenant,
                                                                      idClass,
                                                                      stateClass);
        return result;
    }

    protected DsPropertyStorage createPropertyStorage() {
        DsPropertyStorage propertyStorage = DsPropertyStorage.newInstance(getDatastore());
        return propertyStorage;
    }

    /**
     * Performs no action.
     */
    @Override
    public void close() throws Exception {
        // NOP
    }

    /**
     * @return an instance of a wrapper of the passed {@link Datastore}
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
        private NamespaceToTenantIdConverter namespaceToTenantIdConverter;

        private Builder() {
            // Avoid direct initialization
        }

        /**
         * @param datastore the {@link Datastore} to use for the DB interactions
         * @return self for method chaining
         */
        public Builder setDatastore(Datastore datastore) {
            this.datastore = checkNotNull(datastore);
            return this;
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
        public Builder setTypeRegistry(
                ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> typeRegistry) {
            this.typeRegistry = checkNotNull(typeRegistry);
            return this;
        }

        /**
         * Sets a {@link NamespaceToTenantIdConverter} for converting the Datastore namespaces and
         * the {@link io.spine.core.TenantId Tenant IDs} back and forth.
         *
         * <p>Setting this parameter is reasonable (but not required) only if the storage is
         * multitenant. Otherwise, an exception will be thrown on {@linkplain #build() build}.
         *
         * @param converter a custom converter for the Tenant IDs
         * @return self for method chaining
         */
        public Builder setNamespaceToTenantIdConverter(NamespaceToTenantIdConverter converter) {
            this.namespaceToTenantIdConverter = checkNotNull(converter);
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
                checkArgument(namespaceToTenantIdConverter == null,
                              REDUNDANT_TENANT_ID_CONVERTER_ERROR_MESSAGE);
            }
            if (namespaceToTenantIdConverter != null) {
                ProjectId projectId = ProjectId.of(datastore);
                TenantConverterRegistry.registerNamespaceConverter(projectId,
                                                                   namespaceToTenantIdConverter);
            }

            return new DatastoreStorageFactory(this);
        }

        private NamespaceSupplier createNamespaceSupplier() {
            String defaultNamespace;
            if (multitenant) {
                checkHasNoNamespace(datastore);
                defaultNamespace = null;
            } else {
                defaultNamespace = datastore.getOptions()
                                            .getNamespace();
            }
            NamespaceSupplier result = NamespaceSupplier.instance(multitenant,
                                                                        defaultNamespace,
                                                                        ProjectId.of(datastore));
            return result;
        }

        private static void checkHasNoNamespace(Datastore datastore) {
            checkNotNull(datastore);
            DatastoreOptions options = datastore.getOptions();
            String namespace = options.getNamespace();
            checkArgument(isNullOrEmpty(namespace),
                          DEFAULT_NAMESPACE_ERROR_MESSAGE);
        }
    }
}
