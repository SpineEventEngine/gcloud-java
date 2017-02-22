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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.spine3.base.Event;
import org.spine3.base.EventContext;
import org.spine3.base.EventId;
import org.spine3.base.FieldFilter;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.Timestamps;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStorage;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.event.storage.EventStorageRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.Filter;
import static com.google.cloud.datastore.StructuredQuery.OrderBy.asc;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.lt;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;
import static org.spine3.base.Stringifiers.idToString;
import static org.spine3.protobuf.Timestamps.convertToNanos;
import static org.spine3.server.event.storage.EventField.event_type;
import static org.spine3.server.storage.EntityField.timestamp_nanos;
import static org.spine3.server.storage.datastore.DatastoreIdentifiers.of;
import static org.spine3.server.storage.datastore.DatastoreProperties.addAggregateIdProperty;
import static org.spine3.server.storage.datastore.DatastoreProperties.addEventTypeProperty;
import static org.spine3.server.storage.datastore.DatastoreProperties.addTimestampNanosProperty;
import static org.spine3.server.storage.datastore.DatastoreProperties.addTimestampProperty;
import static org.spine3.server.storage.datastore.DatastoreProperties.makeEventContextProperties;
import static org.spine3.server.storage.datastore.DatastoreProperties.makeEventFieldProperties;
import static org.spine3.server.storage.datastore.Entities.entityToMessage;
import static org.spine3.server.storage.datastore.Entities.messageToEntity;

/**
 * Storage for event records based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @author Dmytro Dashenkov
 * @see DatastoreStorageFactory
 */
@SuppressWarnings("WeakerAccess")   // Part of API
public class DsEventStorage extends EventStorage {

    private final DatastoreWrapper datastore;
    private static final String KIND = EventStorageRecord.class.getName();
    private static final TypeUrl RECORD_TYPE_URL = TypeUrl.from(EventStorageRecord.getDescriptor());

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

    private static final Comparator<EventStorageRecord> EVENT_RECORD_HISTORICAL_COMPARATOR =
            new Comparator<EventStorageRecord>() {
                @Override
                public int compare(EventStorageRecord left, EventStorageRecord right) {
                    return Timestamps.compare(left.getTimestamp(), right.getTimestamp());
                }
            };

    private static final Function<Any, String> ID_TRANSFORMER = new Function<Any, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Any input) {
            if (input == null) {
                return null;
            }

            return idToString(input);
        }
    };

    public DsEventStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
    }

    private static Predicate<EventStorageRecord> eventPredicate(
            @SuppressWarnings("TypeMayBeWeakened") EventStreamQuery query) {
        return new EventPredicate(query.getFilterList());
    }

    @Override
    public Iterator<Event> iterator(EventStreamQuery eventStreamQuery) {
        final Iterable<Query<Entity>> queries = toEventIteratorQueries(eventStreamQuery);

        final Collection<Entity> entities = new LinkedList<>();
        for (Query<Entity> query : queries) {
            entities.addAll(datastore.read(query));
        }
        // Transform and filter order does not matter since both operations are performed lazily
        Collection<EventStorageRecord> events = Collections2.transform(entities, ENTITY_TO_EVENT_RECORD);
        events = Collections2.filter(events, eventPredicate(eventStreamQuery));
        final List<EventStorageRecord> sortedEvents = new ArrayList<>(events);
        Collections.sort(sortedEvents, EVENT_RECORD_HISTORICAL_COMPARATOR);

        final Iterator<EventStorageRecord> iterator = events.iterator();
        return toEventIterator(iterator);
    }

    private static Iterable<Query<Entity>> toEventIteratorQueries(EventStreamQuery query) {
        final long lower = convertToNanos(query.getAfter());
        final long upper = query.hasBefore()
                           ? convertToNanos(query.getBefore())
                           : Long.MAX_VALUE;
        final Filter greaterThen = gt(timestamp_nanos.toString(), lower);
        final Filter lessThen = lt(timestamp_nanos.toString(), upper);
        final Filter timeFilter = and(greaterThen, lessThen);

        final Collection<Query<Entity>> result = new HashSet<>(query.getFilterCount());
        for (EventFilter eventFilter : query.getFilterList()) {
            final String eventType = eventFilter.getEventType();
            if (eventType.isEmpty()) {
                continue;
            }
            final Filter typeFilter = eq(event_type.toString(), eventType);
            final Filter filter = and(typeFilter, timeFilter);
            final Query<Entity> queryForType = queryWithFilter(filter);
            result.add(queryForType);
        }
        if (result.isEmpty()) {
            final Query<Entity> queryForAllTypes = queryWithFilter(timeFilter);
            return singleton(queryForAllTypes);
        }

        return result;
    }

    private static Query<Entity> queryWithFilter(Filter filter) {
        final Query<Entity> query = Query.newEntityQueryBuilder()
                                         .setKind(KIND)
                                         .setFilter(filter)
                                         .addOrderBy(asc(timestamp_nanos.toString()))
                                         .build();
        return query;
    }

    @Override
    protected void writeRecord(EventStorageRecord record) {
        final Key key = DatastoreIdentifiers.keyFor(datastore, KIND, of(record));

        final Entity entity = messageToEntity(record, key);

        final Entity.Builder builder = Entity.newBuilder(entity);
        addTimestampProperty(record.getTimestamp(), builder);
        addTimestampNanosProperty(record.getTimestamp(), builder);

        final Message aggregateId = AnyPacker.unpack(record.getContext()
                                                           .getProducerId());
        addAggregateIdProperty(aggregateId, builder);
        addEventTypeProperty(record.getEventType(), builder);
        makeEventContextProperties(record.getContext(), builder);
        makeEventFieldProperties(record, builder);

        datastore.createOrUpdate(builder.build());
    }

    @Nullable
    @Override
    protected Optional<EventStorageRecord> readRecord(EventId eventId) {
        final Key key = DatastoreIdentifiers.keyFor(datastore, KIND, of(eventId));
        final Entity response = datastore.read(key);

        if (response == null) {
            return Optional.absent();
        }

        final EventStorageRecord result = entityToMessage(response, RECORD_TYPE_URL);
        return Optional.of(result);
    }

    /**
     * Provides an access to the GAE Datastore with an API, specific to the Spine framework.
     *
     * <p>Allows the customization of the storage behavior in descendants.
     *
     * @return the wrapped instance of Datastore
     */
    protected DatastoreWrapper getDatastore() {
        return datastore;
    }

    private static class EventPredicate implements Predicate<EventStorageRecord> {

        private final Collection<EventFilter> eventFilters;

        private EventPredicate(Collection<EventFilter> eventFilters) {
            this.eventFilters = eventFilters;
        }

        @Override
        public boolean apply(@Nullable EventStorageRecord event) {
            if (event == null) {
                return false;
            }

            if (eventFilters.isEmpty()) {
                return true;
            }

            boolean atLeastOneNonEmptyFilter = false;
            for (EventFilter filter : eventFilters) {
                final EventFilterChecker predicate = new EventFilterChecker(filter);
                if (predicate.checkFilterEmpty()) {
                    continue;
                }
                atLeastOneNonEmptyFilter = true;
                final boolean matches = predicate.apply(event);
                if (matches) {
                    return true;
                }
            }

            return !atLeastOneNonEmptyFilter;
        }
    }

    /**
     * Predicate matching an {@link EventStorageRecord} to the given {@link EventFilter}.
     */
    private static class EventFilterChecker implements Predicate<EventStorageRecord> {

        private final Collection<String> aggregateIds;
        private final Collection<FieldFilter> eventFieldFilters;
        private final Collection<FieldFilter> contextFieldFilters;

        private static final Function<Any, Message> ANY_UNPACKER = new Function<Any, Message>() {
            @Nullable
            @Override
            public Message apply(@Nullable Any input) {
                if (input == null) {
                    return null;
                }

                return AnyPacker.unpack(input);
            }
        };

        private EventFilterChecker(@SuppressWarnings("TypeMayBeWeakened") EventFilter eventFilter) {
            this.aggregateIds = Collections2.transform(eventFilter.getAggregateIdList(), ID_TRANSFORMER);
            this.eventFieldFilters = eventFilter.getEventFieldFilterList();
            this.contextFieldFilters = eventFilter.getContextFieldFilterList();
        }

        private boolean checkFilterEmpty() {
            return aggregateIds.isEmpty()
                    && eventFieldFilters.isEmpty()
                    && contextFieldFilters.isEmpty();
        }

        // Defined as nullable, parameter `event` is actually non null.
        @SuppressWarnings({"NullableProblems", "MethodWithMoreThanThreeNegations", "MethodWithMultipleLoops"})
        @Override
        public boolean apply(@Nonnull EventStorageRecord event) {
            final Any eventWrapped = event.getMessage();
            final Message eventMessage = AnyPacker.unpack(eventWrapped);

            // Check aggregate ID
            final String aggregateId = event.getProducerId();
            final boolean idMatches = aggregateIds.isEmpty()
                    || aggregateIds.contains(aggregateId);
            if (!idMatches) {
                return false;
            }

            // Check event fields
            for (FieldFilter filter : eventFieldFilters) {
                final boolean matchesFilter = checkFields(eventMessage, filter);
                if (!matchesFilter) {
                    return false;
                }
            }

            // Check context fields
            final EventContext context = event.getContext();
            for (FieldFilter filter : contextFieldFilters) {
                final boolean matchesFilter = checkFields(context, filter);
                if (!matchesFilter) {
                    return false;
                }
            }

            return true;
        }

        private static boolean checkFields(Message object, FieldFilter filter) {
            final String fieldPath = filter.getFieldPath();
            final String fieldName = fieldPath.substring(fieldPath.lastIndexOf('.') + 1);
            checkArgument(!Strings.isNullOrEmpty(fieldName), "Field filter " + filter.toString() + " is invalid");
            final String fieldGetterName = "get" + fieldName.substring(0, 1)
                                                            .toUpperCase() + fieldName.substring(1);

            final Collection<Any> expectedAnys = filter.getValueList();
            final Collection<Message> expectedValues = Collections2.transform(expectedAnys, ANY_UNPACKER);
            Message actualValue;
            try {
                final Class<?> messageClass = object.getClass();
                final Method fieldGetter = messageClass.getDeclaredMethod(fieldGetterName);
                actualValue = (Message) fieldGetter.invoke(object);
                if (actualValue instanceof Any) {
                    actualValue = AnyPacker.unpack((Any) actualValue);
                }
            } catch (@SuppressWarnings("OverlyBroadCatchBlock") ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }

            final boolean result = expectedValues.contains(actualValue);
            return result;
        }
    }
}
