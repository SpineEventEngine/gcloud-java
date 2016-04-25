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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;
import java.util.Iterator;

import static com.google.api.services.datastore.DatastoreV1.*;
import static com.google.api.services.datastore.DatastoreV1.PropertyOrder.Direction.ASCENDING;
import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.datastore.DatastoreWrapper.*;

/**
 * Storage for event records based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsEventStorage extends EventStorage {

    private final DatastoreWrapper datastore;
    private static final String KIND = EventStorageRecord.class.getName();
    private static final String TYPE_URL = TypeName.of(EventStorageRecord.getDescriptor()).toTypeUrl();

    /* package */ static DsEventStorage newInstance(DatastoreWrapper datastore) {
        return new DsEventStorage(datastore);
    }

    private DsEventStorage(DatastoreWrapper datastore) {
        this.datastore = datastore;
    }

    @Override
    public Iterator<Event> iterator(EventStreamQuery eventStreamQuery) {

        final Query.Builder query = DatastoreQueries
                .makeQuery(ASCENDING, KIND, eventStreamQuery);
        final Iterator<EntityResult> iterator = datastore.runQueryForIterator(query.build());
        final Iterator<Event> transformedIterator = Iterators.transform(iterator, ENTITY_TO_EVENT);

        return transformedIterator;
    }

    @Override
    protected void writeInternal(EventStorageRecord record) {
        final Key.Builder key = makeKey(KIND, record.getEventId());
        final Entity.Builder entity = messageToEntity(record, key);
        entity.addProperty(DatastoreProperties.makeTimestampProperty(record.getTimestamp()));
        entity.addProperty(DatastoreProperties.makeTimestampNanosProperty(record.getTimestamp()));
        entity.addProperty(DatastoreProperties.makeAggregateIdProperty(record.getContext().getProducerId()));
        entity.addProperty(DatastoreProperties.makeEventTypeProperty(record.getEventType()));

        entity.addAllProperty(DatastoreProperties.makeEventContextProperties(record.getContext()));
        entity.addAllProperty(DatastoreProperties.makeEventFieldProperties(record));

        final Mutation.Builder mutation = Mutation.newBuilder().addUpsert(entity);
        datastore.commit(mutation);
    }

    @Nullable
    @Override
    protected EventStorageRecord readInternal(EventId eventId) {
        final String idString = idToString(eventId);
        final Key.Builder key = makeKey(KIND, idString);
        final LookupRequest request = LookupRequest.newBuilder().addKey(key).build();

        final LookupResponse response = datastore.lookup(request);

        if (response == null || response.getFoundCount() == 0) {
            return null;
        }

        final EntityResult entity = response.getFound(0);
        final EventStorageRecord result = entityToMessage(entity, TYPE_URL);
        return result;
    }

    private static final Function<EntityResult, Event> ENTITY_TO_EVENT = new Function<EntityResult, Event>() {
        @Nullable
        @Override
        public Event apply(@Nullable EntityResult entityResult) {
            if (entityResult == null) {
                return Event.getDefaultInstance();
            }
            final EventStorageRecord message = entityToMessage(entityResult, TYPE_URL);
            final Event result = toEvent(message);
            return result;
        }
    };
}
