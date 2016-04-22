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

import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreOptions;
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

    /**
     * @param datastore the {@link Datastore} implementation to use.
     * @return creates new factory instance.
     * @see DatastoreOptions
     */
    /* package */ static DatastoreStorageFactory newInstance(Datastore datastore) {
        return new DatastoreStorageFactory(datastore);
    }

    /* package */ DatastoreStorageFactory(Datastore datastore) {
        this.datastore = new DatastoreWrapper(datastore);
    }

    @Override
    public CommandStorage createCommandStorage() {
        return DsCommandStorage.newInstance(datastore);
    }

    @Override
    public EventStorage createEventStorage() {
        return DsEventStorage.newInstance(datastore);
    }

    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(Class<? extends Entity<I, ?>> aClass) {
        final DsEntityStorage<I> entityStorage = (DsEntityStorage<I>) createEntityStorage(aClass);
        final DsPropertyStorage propertyStorage = DsPropertyStorage.newInstance(datastore);
        return DsProjectionStorage.newInstance(entityStorage, propertyStorage, aClass);
    }

    @Override
    public <I> EntityStorage<I> createEntityStorage(Class<? extends Entity<I, ?>> entityClass) {
        final Class<Message> messageClass = getGenericParameterType(entityClass, ENTITY_MESSAGE_TYPE_PARAMETER_INDEX);
        final Descriptor descriptor = (Descriptor) getClassDescriptor(messageClass);
        return DsEntityStorage.newInstance(descriptor, datastore);
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(Class<? extends Aggregate<I, ?, ?>> ignored) {
        return DsAggregateStorage.newInstance(datastore, DsPropertyStorage.newInstance(datastore));
    }

    @SuppressWarnings("ProhibitedExceptionDeclared")
    @Override
    public void close() throws Exception {
        // NOP
    }
}
