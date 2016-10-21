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
import com.google.cloud.datastore.GqlQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.Messages;
import org.spine3.protobuf.Timestamps;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.event.EventStreamQueryOrBuilder;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.datastore.newapi.DatastoreWrapper;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

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
    private static final int SQL_INITIAL_LENGTH = 128;

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

    private static final Function<Any, String> ID_TRANSFORMER = new Function<Any, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Any input) {
            if (input == null) {
                return null;
            }

            final Message messageId = AnyPacker.unpack(input);
            return Messages.toText(messageId);
        }
    };

    /* package */
    static DsEventStorage newInstance(DatastoreWrapper datastore, boolean multitenant) {
        return new DsEventStorage(datastore, multitenant);
    }

    private DsEventStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
    }

    private static Predicate<Event> eventPredicate(EventStreamQuery query) {
        final Collection<String> eventTypes = new HashSet<>(query.getFilterCount());
        final Collection<String> aggregateIds = new HashSet<>(query.getFilterCount());

        for (EventFilter filter : query.getFilterList()) {
            final Collection<String> stringIds = Collections2.transform(filter.getAggregateIdList(), ID_TRANSFORMER);
            aggregateIds.addAll(stringIds);
            eventTypes.add(filter.getEventType());
        }

        // TODO:21-10-16:dmytro.dashenkov: Field filters (for both context and message).

        return new EventPredicate(eventTypes, aggregateIds);
    }

    @Override
    public Iterator<Event> iterator(EventStreamQuery eventStreamQuery) {
        final Query query = toTimestampQuery(eventStreamQuery);

        final Collection<Entity> entities = datastore.read(query);
        // TODO:21-10-16:dmytro.dashenkov: Not optimal. Should first filter then transform.
        Collection<Event> events = Collections2.transform(entities, ENTITY_TO_EVENT);
        events = Collections2.filter(events, eventPredicate(eventStreamQuery));

        final Iterator<Event> iterator = events.iterator();
        return iterator;

    }

    @SuppressWarnings({"MethodWithMoreThanThreeNegations", "ValueOfIncrementOrDecrementUsed", "DuplicateStringLiteralInspection"})
    private static Query toTimestampQuery(EventStreamQueryOrBuilder query) {
        final long lower = Timestamps.convertToNanos(query.getAfter());
        final long upper = query.hasBefore()
                ? Timestamps.convertToNanos(query.getBefore())
                : Long.MAX_VALUE;

        final StringBuilder sql = new StringBuilder(SQL_INITIAL_LENGTH);
        sql.append("SELECT * FROM ")
                .append(TYPE_URL.getSimpleName())
                .append(" WHERE ")
                .append(TIMESTAMP_NANOS_PROPERTY_NAME)
                .append(" > @1")
                .append(" AND ")
                .append(TIMESTAMP_NANOS_PROPERTY_NAME)
                .append(" < @2");
        final GqlQuery.Builder gqlQuery = Query.gqlQueryBuilder(sql.toString())
                .addBinding(lower)
                .addBinding(upper);
        return gqlQuery.build();
    }

    @Override
    protected void writeRecord(EventStorageRecord record) {
        final Key key = datastore.getKeyFactory(KIND).newKey(record.getEventId());
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
        final Key key = datastore.getKeyFactory(TYPE_URL.getSimpleName()).newKey(idString);

        final Entity response = datastore.read(key);

        if (response == null) {
            return null;
        }

        final EventStorageRecord result = entityToMessage(response, TYPE_URL);
        return result;
    }

    private static class EventPredicate implements Predicate<Event> {

        private final Collection<String> eventTypes;
        private final Collection<String> aggregateIds;

        private EventPredicate(Collection<String> eventTypes, Collection<String> aggregateIds) {
            this.eventTypes = eventTypes;
            this.aggregateIds = aggregateIds;
        }

        @Override
        public boolean apply(@Nullable Event event) {
            if (event == null) {
                return false;
            }

            final Any eventAny = event.getMessage();
            final Message eventMessage = AnyPacker.unpack(eventAny);
            final String eventType = eventMessage.getDescriptorForType().getFullName();

            final boolean typeMatches = eventTypes.contains(eventType);
            if (!typeMatches) {
                return false;
            }

            if (!aggregateIds.isEmpty()) {
                final Any aggregateIdAny = event.getContext().getProducerId();
                final Message aggregateId = AnyPacker.unpack(aggregateIdAny);
                final String aggregateIdString = Messages.toText(aggregateId);

                final boolean idMatches = aggregateIds.contains(aggregateIdString);
                if (!idMatches) {
                    return false;
                }
            }

            return true;
        }
    }
}
