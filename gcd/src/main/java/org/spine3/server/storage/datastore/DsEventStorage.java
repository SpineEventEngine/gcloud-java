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
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.base.Identifiers;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.Timestamps;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.event.EventStreamQueryOrBuilder;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.datastore.DatastoreProperties.TIMESTAMP_NANOS_PROPERTY_NAME;
import static org.spine3.server.storage.datastore.Entities.entityToMessage;
import static org.spine3.server.storage.datastore.Entities.messageToEntity;

/**
 * Storage for event records based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @author Dmytro Dashenkov
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsEventStorage extends EventStorage {

    private final DatastoreWrapper datastore;
    private static final String KIND = EventStorageRecord.class.getName();
    private static final TypeUrl RECORD_TYPE_URL = TypeUrl.of(EventStorageRecord.getDescriptor());

    private static final Function<Entity, EventStorageRecord> ENTITY_TO_EVENT_RECORD
            = new Function<Entity, EventStorageRecord>() {
        @Nullable
        @Override
        public EventStorageRecord apply(@Nullable Entity entityResult) {
            if (entityResult == null) {
                return EventStorageRecord.getDefaultInstance();
            }
            final EventStorageRecord message = entityToMessage(entityResult, RECORD_TYPE_URL);
            return message;
        }
    };

    private static final Function<Any, String> ID_TRANSFORMER = new Function<Any, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Any input) {
            if (input == null) {
                return null;
            }

            return Identifiers.idToString(input);
        }
    };

    /* package */ static DsEventStorage newInstance(DatastoreWrapper datastore, boolean multitenant) {
        return new DsEventStorage(datastore, multitenant);
    }

    private DsEventStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
    }

    private static Predicate<EventStorageRecord> eventPredicate(
            @SuppressWarnings("TypeMayBeWeakened") EventStreamQuery query) {
        return new EventPredicate(query.getFilterList());
    }

    @Override
    public Iterator<Event> iterator(EventStreamQuery eventStreamQuery) {
        final Query query = toTimestampQuery(eventStreamQuery);

        final Collection<Entity> entities = datastore.read(query);
        // Transform and filter order does not matter since both operations are performed lazily
        Collection<EventStorageRecord> events = Collections2.transform(entities, ENTITY_TO_EVENT_RECORD);
        events = Collections2.filter(events, eventPredicate(eventStreamQuery));

        final Iterator<EventStorageRecord> iterator = events.iterator();
        return toEventIterator(iterator);
    }

    @SuppressWarnings({"MethodWithMoreThanThreeNegations", "ValueOfIncrementOrDecrementUsed", "DuplicateStringLiteralInspection"})
    private static Query toTimestampQuery(EventStreamQueryOrBuilder query) {
        final long lower = Timestamps.convertToNanos(query.getAfter());
        final long upper = query.hasBefore()
                           ? Timestamps.convertToNanos(query.getBefore())
                           : Long.MAX_VALUE;
        final PropertyFilter greaterThen = PropertyFilter.gt(TIMESTAMP_NANOS_PROPERTY_NAME, lower);
        final PropertyFilter lessThen = PropertyFilter.lt(TIMESTAMP_NANOS_PROPERTY_NAME, upper);
        final CompositeFilter filter = CompositeFilter.and(greaterThen, lessThen);
        return Query.newEntityQueryBuilder()
                    .setKind(KIND)
                    .setFilter(filter)
                    .build();
    }

    @Override
    protected void writeRecord(EventStorageRecord record) {
        final Key key = Keys.generateForKindWithName(datastore, KIND, record.getEventId());

        final Entity entity = messageToEntity(record, key);

        final Entity.Builder builder = Entity.newBuilder(entity);
        DatastoreProperties.addTimestampProperty(record.getTimestamp(), builder);
        DatastoreProperties.addTimestampNanosProperty(record.getTimestamp(), builder);

        final Message aggregateId = AnyPacker.unpack(record.getContext().getProducerId());
        DatastoreProperties.addAggregateIdProperty(aggregateId, builder);
        DatastoreProperties.addEventTypeProperty(record.getEventType(), builder);
        DatastoreProperties.makeEventContextProperties(record.getContext(), builder);
        DatastoreProperties.makeEventFieldProperties(record, builder);

        datastore.createOrUpdate(builder.build());
    }

    @Nullable
    @Override
    protected EventStorageRecord readRecord(EventId eventId) {
        final String idString = idToString(eventId);
        final Key key = Keys.generateForKindWithName(datastore, KIND, idString);
        final Entity response = datastore.read(key);

        if (response == null) {
            return null;
        }

        final EventStorageRecord result = entityToMessage(response, RECORD_TYPE_URL);
        return result;
    }

    private static class EventPredicate implements Predicate<EventStorageRecord> {

        private final Collection<EventFilter> eventFilters;

        private EventPredicate(Collection<EventFilter> eventFilters) {
            this.eventFilters = eventFilters;
        }

        @SuppressWarnings("MethodWithMoreThanThreeNegations")
        @Override
        public boolean apply(@Nullable EventStorageRecord event) {
            if (event == null) {
                return false;
            }

            if (eventFilters.isEmpty()) {
                return true;
            }

            for (EventFilter filter : eventFilters) {
                final EventFilterChecker predicate = new EventFilterChecker(filter);
                if (predicate.isDefault()) {
                    continue;
                }
                final boolean matches = predicate.apply(event);
                if (matches) {
                    return true;
                }
            }

            return false;
        }
    }

    private static class EventFilterChecker implements Predicate<EventStorageRecord> {

        // TODO:21-10-16:dmytro.dashenkov: Field filters (for both context and message).

        private final String eventType;
        private final Collection<String> aggregateIds;

        private EventFilterChecker(@SuppressWarnings("TypeMayBeWeakened") EventFilter eventFilter) {
            this.eventType = checkNotNull(eventFilter.getEventType());
            this.aggregateIds = Collections2.transform(eventFilter.getAggregateIdList(), ID_TRANSFORMER);
        }

        private boolean isDefault() {
            return eventType.trim().isEmpty() && aggregateIds.isEmpty();
        }

        @SuppressWarnings("NullableProblems") // Defined as nullable, parameter `event` is actually non null.
        @Override
        public boolean apply(@Nonnull EventStorageRecord event) {
            if (eventType.trim()
                         .isEmpty() && aggregateIds.isEmpty()) {
                return true;
            }

            final Any eventWrapped = event.getMessage();
            final Message eventMessage = AnyPacker.unpack(eventWrapped);
            final String actualType = eventMessage.getDescriptorForType()
                                                  .getFullName();
            if (!eventType.isEmpty() && !eventType.equals(actualType)) {
                return false;
            }

            if (aggregateIds.isEmpty()) {
                return true;
            }

            final String aggregateId = event.getProducerId();

            final boolean idMatches = aggregateIds.contains(aggregateId);
            return idMatches;
        }
    }
}
