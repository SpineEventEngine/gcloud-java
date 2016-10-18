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
import com.google.datastore.v1.*;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.datastore.newapi.DatastoreWrapper;

import javax.annotation.Nullable;
import java.util.Iterator;

import static com.google.datastore.v1.PropertyOrder.Direction.ASCENDING;
import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static org.spine3.base.Identifiers.idToString;

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
    private static final String TYPE_URL = TypeUrl.of(EventStorageRecord.getDescriptor()).value();

    /* package */ static DsEventStorage newInstance(DatastoreWrapper datastore, boolean multitenant) {
        return new DsEventStorage(datastore, multitenant);
    }

    private DsEventStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
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
    protected void writeRecord(EventStorageRecord record) {
        final Key.Builder key = makeKey(KIND, record.getEventId());
        final Entity.Builder entity = messageToEntity(record, key);
        DatastoreProperties.addTimestampProperty(record.getTimestamp(), entity);
        DatastoreProperties.addTimestampNanosProperty(record.getTimestamp(), entity);
        DatastoreProperties.addAggregateIdProperty(record.getContext().getProducerId(), entity);
        DatastoreProperties.addEventTypeProperty(record.getEventType(), entity);

        DatastoreProperties.makeEventContextProperties(record.getContext(), entity);
        DatastoreProperties.makeEventFieldProperties(record, entity);

        WriteOperations.createOrUpdate(entity.build(), datastore);
    }

    @Nullable
    @Override
    protected EventStorageRecord readRecord(EventId eventId) {
        final String idString = idToString(eventId);
        final Key.Builder key = makeKey(KIND, idString);
        final LookupRequest request = LookupRequest.newBuilder().addKeys(key).build();

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
