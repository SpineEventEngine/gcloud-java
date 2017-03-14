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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.command.CommandStorage;
import org.spine3.server.entity.Entity;
import org.spine3.server.event.EventStorage;
import org.spine3.server.projection.ProjectionStorage;
import org.spine3.server.reflect.Classes;
import org.spine3.server.stand.StandStorage;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.Storage;
import org.spine3.server.storage.StorageFactory;

import static com.google.common.base.Preconditions.checkState;
import static org.spine3.protobuf.Messages.getClassDescriptor;
import static org.spine3.server.reflect.Classes.getGenericParameterType;

/**
 * Creates storages based on GAE {@link Datastore}.
 *
 * @author Alexander Litus
 * @author Mikhail Mikhaylov
 */
@SuppressWarnings("WeakerAccess") // Part of API
public class DatastoreStorageFactory implements StorageFactory {

    private static final int ENTITY_MESSAGE_TYPE_PARAMETER_INDEX = 1;

    private DatastoreWrapper datastore;
    private final boolean multitenant;

    /**
     * Creates new instance of {@code DatastoreStorageFactory}.
     *
     * @param datastore   the {@link Datastore} implementation to use
     * @param multitenant if storage factory is configured to serve a multitenant application.
     *                    This implementation does not support
     *                    <a href="https://cloud.google.com/appengine/docs/java/multitenancy/multitenancy">Datastore
     *                    recommended</a> way of creating multitenant apps yet.
     *                    See {@link Storage#isMultitenant()}.
     */
    @SuppressWarnings({"OverridableMethodCallDuringObjectConstruction", "OverriddenMethodCallDuringObjectConstruction"})
    public DatastoreStorageFactory(Datastore datastore, boolean multitenant) {
        this.multitenant = multitenant;
        initDatastoreWrapper(datastore);
    }

    /**
     * Creates new instance of non-multitenant {@code DatastoreStorageFactory}.
     *
     * @param datastore the {@link Datastore} implementation to use
     */
    public DatastoreStorageFactory(Datastore datastore) {
        this(datastore, false);
    }

    @VisibleForTesting
    protected void initDatastoreWrapper(Datastore datastore) {
        checkState(this.getDatastore() == null, "Datastore is already initialized");
        final DatastoreWrapper wrapped = DatastoreWrapper.wrap(datastore);
        this.setDatastore(wrapped);
    }

    @Override
    public boolean isMultitenant() {
        return multitenant;
    }

    @Override
    public CommandStorage createCommandStorage() {
        final DsCommandStorage result = new DsCommandStorage(getDatastore(), multitenant);
        return result;
    }

    @Override
    public EventStorage createEventStorage() {
        final DsEventStorage result = new DsEventStorage(getDatastore(), multitenant);
        return result;
    }

    @Override
    public StandStorage createStandStorage() {
        final DsRecordStorage<DatastoreRecordId> recordStorage
                = (DsRecordStorage<DatastoreRecordId>) createRecordStorage(StandStorageRecord.class);
        final DsStandStorage result = new DsStandStorage(recordStorage, multitenant);
        return result;
    }

    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(Class<? extends Entity<I, ?>> aClass) {
        final DsRecordStorage<I> entityStorage = (DsRecordStorage<I>) createRecordStorage(aClass);
        final DsPropertyStorage propertyStorage = createPropertyStorage();
        final DsProjectionStorage<I> result = new DsProjectionStorage<>(entityStorage,
                                                                        propertyStorage,
                                                                        aClass,
                                                                        multitenant);
        return result;
    }

    @Override
    public <I> RecordStorage<I> createRecordStorage(Class<? extends Entity<I, ?>> entityClass) {
        final Class<Message> messageClass = getGenericParameterType(entityClass, ENTITY_MESSAGE_TYPE_PARAMETER_INDEX);
        final Descriptor descriptor = (Descriptor) getClassDescriptor(messageClass);
        final Class<I> idClass = Classes.getGenericParameterType(entityClass, Entity.GenericParameter.ID.getIndex());
        final DsRecordStorage<I> result = new DsRecordStorage<>(descriptor, getDatastore(), multitenant, idClass);
        return result;
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(Class<? extends Aggregate<I, ?, ?>> ignored) {
        final DsPropertyStorage propertyStorage = createPropertyStorage();
        final DsAggregateStorage<I> result = new DsAggregateStorage<>(getDatastore(), propertyStorage, multitenant);
        return result;
    }

    protected DsPropertyStorage createPropertyStorage() {
        final DsPropertyStorage propertyStorage = DsPropertyStorage.newInstance(getDatastore());
        return propertyStorage;
    }

    @Override
    public void close() throws Exception {
        // NOP
    }

    protected DatastoreWrapper getDatastore() {
        return datastore;
    }

    protected void setDatastore(DatastoreWrapper datastore) {
        this.datastore = datastore;
    }

}
