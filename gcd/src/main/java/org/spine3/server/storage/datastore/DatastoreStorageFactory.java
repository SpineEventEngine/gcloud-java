/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.*;

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
     * <p>Same as calling {@link #newInstance(Datastore, boolean)} with {@code false} second argument.
     *
     * @param datastore the {@link Datastore} implementation to use.
     * @return new instance of the {@code DatastoreStorageFactory}.
     * @see DatastoreOptions
     */
    @SuppressWarnings("WeakerAccess") // Part of API
    public static DatastoreStorageFactory newInstance(Datastore datastore) {
        return new DatastoreStorageFactory(datastore, false);
    }

    /**
     * Creates new instance of {@code DatastoreStorageFactory}.
     *
     * @param datastore   the {@link Datastore} implementation to use.
     * @param multitenant shows if storage factory is configured to serve a multitenant application
     * @return creates new factory instance.
     * @see DatastoreOptions
     */
    @SuppressWarnings("WeakerAccess") // Part of API
    public static DatastoreStorageFactory newInstance(Datastore datastore, boolean multitenant) {
        return new DatastoreStorageFactory(datastore, multitenant);
    }

    @SuppressWarnings({"OverridableMethodCallDuringObjectConstruction", "OverriddenMethodCallDuringObjectConstruction"}) // Overriding used for testing
    /* package */DatastoreStorageFactory(Datastore datastore, boolean multitenant) {
        this.multitenant = multitenant;
        initDatastoreWrapper(datastore);
    }

    @VisibleForTesting
    protected void initDatastoreWrapper(Datastore datastore) {
        checkState(this.getDatastore() == null, "Datastore is already init");
        final DatastoreWrapper wrapped = DatastoreWrapper.wrap(datastore);
        this.setDatastore(wrapped);
    }

    @Override
    public boolean isMultitenant() {
        return multitenant;
    }

    @Override
    public CommandStorage createCommandStorage() {
        return DsCommandStorage.newInstance(getDatastore(), multitenant);
    }

    @Override
    public EventStorage createEventStorage() {
        return DsEventStorage.newInstance(getDatastore(), multitenant);
    }

    @Override
    public StandStorage createStandStorage() {
        final DsRecordStorage<String> recordStorage
                = (DsRecordStorage<String>) createRecordStorage(StandStorageRecord.class);
        return DsStandStorage.newInstance(multitenant, recordStorage);
    }

    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(Class<? extends Entity<I, ?>> aClass) {
        final DsRecordStorage<I> entityStorage = (DsRecordStorage<I>) createRecordStorage(aClass);
        final DsPropertyStorage propertyStorage = DsPropertyStorage.newInstance(getDatastore());
        return DsProjectionStorage.newInstance(entityStorage, propertyStorage, aClass, multitenant);
    }

    @Override
    public <I> RecordStorage<I> createRecordStorage(Class<? extends Entity<I, ?>> entityClass) {
        final Class<Message> messageClass = getGenericParameterType(entityClass, ENTITY_MESSAGE_TYPE_PARAMETER_INDEX);
        final Descriptors.Descriptor descriptor = (Descriptors.Descriptor) getClassDescriptor(messageClass);
        return DsRecordStorage.newInstance(descriptor, getDatastore(), multitenant);
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(Class<? extends Aggregate<I, ?, ?>> ignored) {
        return DsAggregateStorage.newInstance(getDatastore(), DsPropertyStorage.newInstance(getDatastore()), multitenant);
    }

    @SuppressWarnings("ProhibitedExceptionDeclared")
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

    private static class StandStorageRecord extends Entity<String, EntityStorageRecord> {

        /**
         * Creates a new instance.
         *
         * @param id the ID for the new instance
         * @throws IllegalArgumentException if the ID is not of one of the supported types for identifiers
         */
        private StandStorageRecord(String id) {
            super(id);
        }
    }
}
