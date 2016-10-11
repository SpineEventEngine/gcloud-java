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

import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreOptions;
import com.google.protobuf.Message;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.*;

import static com.google.protobuf.Descriptors.Descriptor;
import static org.spine3.protobuf.Messages.getClassDescriptor;
import static org.spine3.server.reflect.Classes.getGenericParameterType;

/**
 * Creates storages based on GAE {@link Datastore}.
 *
 * @author Alexander Litus
 * @author Mikhail Mikhaylov
 */
public class DatastoreStorageFactory implements StorageFactory {

    private static final int ENTITY_MESSAGE_TYPE_PARAMETER_INDEX = 1;

    private final DatastoreWrapper datastore;
    private final Options options;
    private final boolean multitenant;

    /**
     * Creates new instance of {@code DatastoreStorageFactory}.
     * <p>Same as calling {@link #newInstance(Datastore, boolean)} with {@code false} second argument.
     *
     * @param datastore the {@link Datastore} implementation to use.
     * @return creates new factory instance.
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

    /* package */ DatastoreStorageFactory(Datastore datastore, boolean multitenant) {
        this.options = new Options();
        this.multitenant = multitenant;
        this.datastore = new DatastoreWrapper(datastore, options);
    }

    @Override
    public boolean isMultitenant() {
        return multitenant;
    }

    @Override
    public CommandStorage createCommandStorage() {
        return DsCommandStorage.newInstance(datastore, multitenant);
    }

    @Override
    public EventStorage createEventStorage() {
        return DsEventStorage.newInstance(datastore, multitenant);
    }

    @Override
    public StandStorage createStandStorage() {
        return null; // TODO:05-10-16:dmytro.dashenkov: Implement.
    }

    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(Class<? extends Entity<I, ?>> aClass) {
        final DsEntityStorage<I> entityStorage = (DsEntityStorage<I>) createRecordStorage(aClass);
        final DsPropertyStorage propertyStorage = DsPropertyStorage.newInstance(datastore);
        return DsProjectionStorage.newInstance(entityStorage, propertyStorage, aClass, multitenant);
    }

    @Override
    public <I> RecordStorage<I> createRecordStorage(Class<? extends Entity<I, ?>> entityClass) {
        final Class<Message> messageClass = getGenericParameterType(entityClass, ENTITY_MESSAGE_TYPE_PARAMETER_INDEX);
        final Descriptor descriptor = (Descriptor) getClassDescriptor(messageClass);
        return DsEntityStorage.newInstance(descriptor, datastore, multitenant);
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(Class<? extends Aggregate<I, ?, ?>> ignored) {
        return DsAggregateStorage.newInstance(datastore, DsPropertyStorage.newInstance(datastore), multitenant);
    }

    @SuppressWarnings("ProhibitedExceptionDeclared")
    @Override
    public void close() throws Exception {
        // NOP
    }

    public Options getOptions() {
        return options;
    }

    @SuppressWarnings("WeakerAccess") // We provide it as API
    public static class Options {
        private Options() {
        }

        private static final int DEFAULT_PAGE_SIZE = 10;

        private int pageSize = DEFAULT_PAGE_SIZE;

        public void setEventIteratorPageSize(int pageSize) {
            if (pageSize < 1) {
                throw new IllegalArgumentException("Events page can not contain less than one event.");
            }

            this.pageSize = pageSize;
        }

        public int getEventIteratorPageSize() {
            return pageSize;
        }
    }
}
