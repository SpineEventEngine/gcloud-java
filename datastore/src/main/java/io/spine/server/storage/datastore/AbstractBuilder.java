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

import com.google.protobuf.Descriptors.Descriptor;
import io.spine.server.entity.Entity;
import io.spine.server.entity.model.EntityClass;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.datastore.type.DatastoreColumnType;
import io.spine.type.TypeUrl;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation base for {@code DsRecordStorage} builders.
 *
 * @param <I> the ID type of the stored entities
 * @param <B> the builder own type
 */
abstract class AbstractBuilder<I, B extends AbstractBuilder<I, B>> {

    private Descriptor descriptor;
    private DatastoreWrapper datastore;
    private boolean multitenant;
    private ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry;
    private Class<I> idClass;
    private Class<? extends io.spine.server.entity.Entity> entityClass;

    /**
     * Prevents direct instantiation.
     */
    AbstractBuilder() {
    }

    /**
     * Sets the type URL of the entity state, which is stored in the resulting storage.
     */
    public B setStateType(TypeUrl stateTypeUrl) {
        checkNotNull(stateTypeUrl);
        Descriptor descriptor = (Descriptor) stateTypeUrl.getDescriptor();
        this.descriptor = checkNotNull(descriptor);
        return self();
    }

    /**
     * Assignts the ID class of the stored entities.
     */
    public B setIdClass(Class<I> idClass) {
        this.idClass = checkNotNull(idClass);
        return self();
    }

    /**
     * Assigns the class of the stored entity.
     */
    public B setEntityClass(Class<? extends io.spine.server.entity.Entity> entityClass) {
        this.entityClass = checkNotNull(entityClass);
        return self();
    }

    /**
     * Assigns the model class to the builder.
     *
     * <p>This call is equivalent of setting {@linkplain #setStateType(io.spine.type.TypeUrl)
     * state type}, {@linkplain #setIdClass(Class) ID class}, and
     * {@linkplain #setEntityClass(Class) entity class} separately.
     */
    @SuppressWarnings("unchecked") // The ID class is ensured by the parameter type.
    public B setModelClass(EntityClass<? extends Entity<I, ?>> modelClass) {
        TypeUrl stateType = modelClass.getStateType();
        Class<I> idClass = (Class<I>) modelClass.getIdClass();

        setStateType(stateType);
        setIdClass(idClass);
        setEntityClass(modelClass.value());

        return self();
    }

    /**
     * Sets the {@link io.spine.server.storage.datastore.DatastoreWrapper} to use in this storage.
     */
    public B setDatastore(DatastoreWrapper datastore) {
        this.datastore = checkNotNull(datastore);
        return self();
    }

    /**
     * Configures multitenancy mode for the storage.
     *
     * @param multitenant {@code true} if the storage should be
     *                    {@link io.spine.server.storage.Storage#isMultitenant multitenant},
     *                    {@code false} otherwise
     */
    public B setMultitenant(boolean multitenant) {
        this.multitenant = multitenant;
        return self();
    }

    /**
     * Assigns the type registry of
     * the {@linkplain io.spine.server.entity.storage.EntityColumn entity columns}.
     */
    public B setColumnTypeRegistry(
            ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> columnTypeRegistry) {
        this.columnTypeRegistry = checkNotNull(columnTypeRegistry);
        return self();
    }

    /**
     * Obtains the {@linkplain com.google.protobuf.Descriptors.Descriptor descriptor}
     * of the stored entity state type.
     */
    public Descriptor getDescriptor() {
        return descriptor;
    }

    /**
     * Obtains the {@link io.spine.server.storage.datastore.DatastoreWrapper} used in this storage.
     */
    public DatastoreWrapper getDatastore() {
        return datastore;
    }

    /**
     * Verifies if the storage is multitenant.
     */
    public boolean isMultitenant() {
        return multitenant;
    }

    /**
     * Obtains the type registry of
     * the {@linkplain io.spine.server.entity.storage.EntityColumn entity columns}.
     */
    public ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> getColumnTypeRegistry() {
        return columnTypeRegistry;
    }

    /**
     * Obtains the ID class of the stored entity.
     */
    public Class<I> getIdClass() {
        return idClass;
    }

    /**
     * Obtains the class of the stored entity.
     */
    public Class<? extends io.spine.server.entity.Entity> getEntityClass() {
        return entityClass;
    }

    final void checkRequiredFields() {
        checkNotNull(descriptor, "State descriptor is not set.");
        checkNotNull(datastore, "Datastore is not set.");
        checkNotNull(columnTypeRegistry, "Column type registry is not set.");
    }

    abstract B self();
}
