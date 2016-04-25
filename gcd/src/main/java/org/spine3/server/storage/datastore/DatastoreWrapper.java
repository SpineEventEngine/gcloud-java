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

import com.google.api.services.datastore.DatastoreV1.*;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.common.base.Function;
import com.google.protobuf.*;

import javax.annotation.Nullable;
import java.util.*;

import static com.google.api.services.datastore.DatastoreV1.CommitRequest.Mode.NON_TRANSACTIONAL;
import static com.google.api.services.datastore.client.DatastoreHelper.*;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static org.spine3.protobuf.Messages.fromAny;
import static org.spine3.protobuf.Messages.toAny;

/**
 * The Google App Engine Cloud {@link Datastore} wrapper.
 *
 * @author Alexander Litus
 * @author Mikhail Mikhaylov
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
/* package */ class DatastoreWrapper {

    private static final String VALUE_PROPERTY_NAME = "value";

    private final Datastore datastore;
    private final DatastoreStorageFactory.Options datastoreOptions;

    /**
     * Creates a new storage instance.
     *
     * @param datastore the datastore implementation to use.
     */
    /* package */ DatastoreWrapper(Datastore datastore, DatastoreStorageFactory.Options datastoreOptions) {
        this.datastore = datastore;
        this.datastoreOptions = datastoreOptions;
    }

    /**
     * Commits the given mutation.
     *
     * @param mutation the mutation to commit.
     * @throws RuntimeException if {@link Datastore#commit(CommitRequest)} throws a {@link DatastoreException}
     */
    private void commit(Mutation mutation) {

        final CommitRequest commitRequest = CommitRequest.newBuilder()
                .setMode(NON_TRANSACTIONAL)
                .setMutation(mutation)
                .build();
        try {
            datastore.commit(commitRequest);
        } catch (DatastoreException e) {
            propagate(e);
        }
    }

    /**
     * Commits the given mutation.
     *
     * @param mutation the mutation to commit.
     * @throws RuntimeException if {@link Datastore#commit(CommitRequest)} throws a {@link DatastoreException}
     */
    @SuppressWarnings("TypeMayBeWeakened") // no, it cannot
    /* package */ void commit(Mutation.Builder mutation) {
        commit(mutation.build());
    }

    /**
     * Commits the {@link LookupRequest}.
     *
     * @param request the request to commit
     * @return the {@link LookupResponse} received
     * @throws RuntimeException if {@link Datastore#lookup(LookupRequest)} throws a {@link DatastoreException}
     */
    /* package */ LookupResponse lookup(LookupRequest request) {
        LookupResponse response = null;
        try {
            response = datastore.lookup(request);
        } catch (DatastoreException e) {
            propagate(e);
        }
        return response;
    }

    /**
     * Runs the given {@link Query}.
     *
     * @param query the query to run.
     * @return the {@link EntityResult} list received or an empty list
     * @throws RuntimeException if {@link Datastore#lookup(LookupRequest)} throws a {@link DatastoreException}
     */
    private List<EntityResult> runQuery(Query query) {
        final RunQueryRequest queryRequest = RunQueryRequest.newBuilder().setQuery(query).build();
        return runQueryRequest(queryRequest);
    }

    /* package */ Iterator<EntityResult> runQueryForIterator(Query query) {
        return new PagingDatastoreIterator(query, this, datastoreOptions.getEventIteratorPageSize());
    }

    /**
     * Runs the {@link Query} got from the given builder.
     *
     * @param query the query builder to build and run.
     * @return the {@link EntityResult} list received or an empty list
     * @throws RuntimeException if {@link Datastore#lookup(LookupRequest)} throws a {@link DatastoreException}
     */
    @SuppressWarnings("TypeMayBeWeakened")
    // no, it cannot
    /* package */ List<EntityResult> runQuery(Query.Builder query) {
        return runQuery(query.build());
    }

    private List<EntityResult> runQueryRequest(RunQueryRequest queryRequest) {
        List<EntityResult> entityResults = newArrayList();
        try {
            entityResults = datastore.runQuery(queryRequest).getBatch().getEntityResultList();
        } catch (DatastoreException e) {
            propagate(e);
        }
        if (entityResults == null) {
            entityResults = newArrayList();
        }
        return entityResults;
    }

    /**
     * Runs the given {@link Query} and returns {@link QueryResultBatch} to provide user with cursor.
     *
     * @param query the query to run.
     * @return query result batch.
     */
    @Nullable
    /* package */ QueryResultBatch runQueryForBatch(Query query) {
        final RunQueryRequest queryRequest = RunQueryRequest.newBuilder().setQuery(query).build();
        QueryResultBatch batch = null;
        try {
            batch = datastore.runQuery(queryRequest).getBatch();
        } catch (DatastoreException e) {
            propagate(e);
        }

        return batch;
    }

    /**
     * Converts the given {@link Message} to the {@link Entity.Builder}.
     *
     * @param message the message to convert
     * @param key     the entity key to set
     * @return the {@link Entity.Builder} with the given key and property created from the serialized message
     */
    /* package */ static Entity.Builder messageToEntity(Message message, Key.Builder key) {
        final ByteString serializedMessage = toAny(message).getValue();
        final Entity.Builder entity = Entity.newBuilder()
                .setKey(key)
                .addProperty(makeProperty(VALUE_PROPERTY_NAME, makeValue(serializedMessage)));
        return entity;
    }

    /**
     * Converts the given {@link EntityResultOrBuilder} to the {@link Message}.
     *
     * @param entity  the entity to convert
     * @param typeUrl the type url of the message
     * @return the deserialized message
     * @see Any#getTypeUrl()
     */
    /* package */ static <M extends Message> M entityToMessage(@Nullable EntityResultOrBuilder entity, String typeUrl) {

        if (entity == null) {
            @SuppressWarnings("unchecked") // cast is safe because Any is Message
            final M empty = (M) Any.getDefaultInstance();
            return empty;
        }

        final Any.Builder any = Any.newBuilder();

        final List<Property> properties = entity.getEntity().getPropertyList();

        for (Property property : properties) {
            if (property.getName().equals(VALUE_PROPERTY_NAME)) {
                any.setValue(property.getValue().getBlobValue());
            }
        }

        any.setTypeUrl(typeUrl);

        final M result = fromAny(any.build());
        return result;
    }

    /**
     * Converts the given {@link EntityResult} list to the {@link Message} list.
     *
     * @param entities the entities to convert
     * @param typeUrl  the type url of the messages
     * @return the deserialized messages
     * @see Any#getTypeUrl()
     */
    /* package */ static <M extends Message> List<M> entitiesToMessages(List<EntityResult> entities, final String typeUrl) {

        final Function<EntityResult, M> entityToMessage = new Function<EntityResult, M>() {
            @Override
            public M apply(@Nullable EntityResult entity) {
                return entityToMessage(entity, typeUrl);
            }
        };
        final List<M> messages = transform(entities, entityToMessage);
        return messages;
    }
}
