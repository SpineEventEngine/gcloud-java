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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.datastore.newapi.DatastoreWrapper;

import javax.annotation.Nullable;
import java.util.Iterator;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import static com.google.cloud.datastore.StructuredQuery.OrderBy;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.datastore.DatastoreProperties.TIMESTAMP_NANOS_PROPERTY_NAME;
import static org.spine3.server.storage.datastore.newapi.Entities.entityToMessage;
import static org.spine3.server.storage.datastore.newapi.Entities.messageToEntity;

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
    private static final TypeUrl TYPE_URL = TypeUrl.of(EventStorageRecord.getDescriptor());

    /* package */
    static DsEventStorage newInstance(DatastoreWrapper datastore, boolean multitenant) {
        return new DsEventStorage(datastore, multitenant);
    }

    private DsEventStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
    }

    @Override
    public Iterator<Event> iterator(EventStreamQuery eventStreamQuery) {
        final Filter afterFilter = PropertyFilter.gt(
                TIMESTAMP_NANOS_PROPERTY_NAME,
                eventStreamQuery.getAfter().getNanos());
        final Filter beforeFilter = PropertyFilter.lt(
                TIMESTAMP_NANOS_PROPERTY_NAME,
                eventStreamQuery.getBefore().getNanos());
        final Filter boundsFilter = CompositeFilter.and(afterFilter, beforeFilter);
        final Query query = Query.entityQueryBuilder()
                .kind(KIND)
                .orderBy(OrderBy.asc(TIMESTAMP_NANOS_PROPERTY_NAME))
                .filter(boundsFilter)
                .build();
        final Iterator<com.google.cloud.datastore.Entity> iterator = datastore.read(query).iterator();
        final Iterator<Event> transformedIterator = Iterators.transform(iterator, ENTITY_TO_EVENT);

        return transformedIterator;
    }

    @Override
    protected void writeRecord(EventStorageRecord record) {
        final Key key = datastore.getKeyFactory().newKey(record.getEventId());
        final Entity entity = messageToEntity(record, key);
        final Entity.Builder builder = Entity.builder(entity);
        DatastoreProperties.addTimestampProperty(record.getTimestamp(), builder);
        DatastoreProperties.addTimestampNanosProperty(record.getTimestamp(), builder);
        DatastoreProperties.addAggregateIdProperty(record.getContext().getProducerId(), builder);
        DatastoreProperties.addEventTypeProperty(record.getEventType(), builder);

        DatastoreProperties.makeEventContextProperties(record.getContext(), builder);
        DatastoreProperties.makeEventFieldProperties(record, builder);

        datastore.createOrUpdate(builder.build());
    }

    @Nullable
    @Override
    protected EventStorageRecord readRecord(EventId eventId) {
        final String idString = idToString(eventId);
        final Key key = datastore.getKeyFactory().newKey(idString);

        final Entity response = datastore.read(key);

        if (response == null) {
            return null;
        }

        final EventStorageRecord result = entityToMessage(response, TYPE_URL);
        return result;
    }

    private static final Function<Entity, Event> ENTITY_TO_EVENT = new Function<Entity, Event>() {
        @Nullable
        @Override
        public Event apply(@Nullable Entity entityResult) {
            if (entityResult == null) {
                return Event.getDefaultInstance();
            }
            final EventStorageRecord message = entityToMessage(entityResult, TYPE_URL);
            final Event result = toEvent(message);
            return result;
        }
    };
}
