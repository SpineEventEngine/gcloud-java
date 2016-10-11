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
import com.google.datastore.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;
import org.spine3.base.FieldFilter;
import org.spine3.base.FieldFilterOrBuilder;
import org.spine3.protobuf.Messages;
import org.spine3.protobuf.Timestamps;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventFilterOrBuilder;
import org.spine3.server.event.EventStreamQueryOrBuilder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.datastore.v1.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1.PropertyFilter.Operator.GREATER_THAN;
import static com.google.datastore.v1.PropertyFilter.Operator.LESS_THAN;
import static com.google.datastore.v1.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static org.spine3.server.storage.datastore.DatastoreProperties.*;

/**
 * Utility class, which simplifies usage of datastore queries.
 *
 * @author Mikhail Mikhaylov
 */
@SuppressWarnings("UtilityClass")
/* package */ class DatastoreQueries {

    private DatastoreQueries() {
    }

    /**
     * Builds a query with the given {@code Entity} kind and the {@code Timestamp} sort direction.
     *
     * @param sortDirection the {@code Timestamp} sort direction
     * @param entityKind    the {@code Entity} kind
     * @return a new {@code Query} instance.
     * @see Entity
     * @see com.google.protobuf.Timestamp
     * @see Query
     */
    /* package */ static Query.Builder makeQuery(PropertyOrder.Direction sortDirection, String entityKind) {
        final Query.Builder query = Query.newBuilder();
        query.addKindBuilder().setName(entityKind);
        query.addOrder(makeOrder(TIMESTAMP_NANOS_PROPERTY_NAME, sortDirection));
        return query;
    }

    /**
     * Builds a query with the given {@code Entity} kind, the {@code Timestamp} sort direction and
     * {@code EventStreamQuery} predicate.
     *
     * @param sortDirection     the {@code Timestamp} sort direction
     * @param entityKind        the {@code Entity} kind
     * @param queryPredicate    the {@code EventStreamQuery} predicate, which specifies additional parameters
     * @return a new {@code Query} instance.
     * @see Entity
     * @see com.google.protobuf.Timestamp
     * @see Query
     */
    /* package */ static Query.Builder makeQuery(
            PropertyOrder.Direction sortDirection,
            String entityKind,
            EventStreamQueryOrBuilder queryPredicate) {
        final Query.Builder query = Query.newBuilder();

        query.addKindBuilder().setName(entityKind);
        query.addOrder(makeOrder(TIMESTAMP_NANOS_PROPERTY_NAME, sortDirection));

        final Filter.Builder beforeFilter = makeFilter(queryPredicate.getBefore(), LESS_THAN);
        final Filter.Builder afterFilter = makeFilter(queryPredicate.getAfter(), GREATER_THAN);

        final List<Filter> filters = new ArrayList<>();
        if (beforeFilter != null) {
            filters.add(beforeFilter.build());
        }
        if (afterFilter != null) {
            filters.add(afterFilter.build());
        }

        filters.addAll(convertEventFilters(queryPredicate.getFilterList()));

        if (filters.size() == 1) {
            query.setFilter(filters.get(0));
        } else if (filters.size() > 1) {
            final CompositeFilter.Builder compositeFilter = CompositeFilter.newBuilder();
            compositeFilter.setOp(CompositeFilter.Operator.AND);

            for (Filter filter : filters) {
                compositeFilter.addFilters(filter);
            }

            query.setFilter(Filter.newBuilder().setCompositeFilter(compositeFilter.build()));
        }

        return query;
    }

    private static Collection<Filter> convertEventFilters(Iterable<EventFilter> eventFilters) {
        final Collection<Filter> filters = new ArrayList<>();

        for (EventFilter eventFilter : eventFilters) {
            addIfNotDefault(filters, convertAggregateIdFilter(eventFilter));
            addIfNotDefault(filters, convertContextFieldFilter(eventFilter));
            addIfNotDefault(filters, convertEventFieldFilter(eventFilter));
            addIfNotDefault(filters, convertEventTypeFilter(eventFilter));
        }

        return filters;
    }

    /**
     * Adds filter to filters collection. Skips adding if filter is default instance.
     *
     * @param filters   mutable filter collection
     * @param newFilter filter to add
     */
    private static void addIfNotDefault(Collection<Filter> filters, Filter newFilter) {
        if (!Filter.getDefaultInstance().equals(newFilter)) {
            filters.add(newFilter);
        }
    }

    private static Filter convertAggregateIdFilter(EventFilterOrBuilder eventFilter) {
        final List<Any> aggregateIds = eventFilter.getAggregateIdList();
        final List<Filter> filters = new ArrayList<>();

        for (Any aggregateId : aggregateIds) {
            filters.add(Filter.newBuilder().setPropertyFilter(PropertyFilter.newBuilder()
                    .setProperty(PropertyReference.newBuilder().setName(AGGREGATE_ID_PROPERTY_NAME))
                    .setOp(EQUAL)
                    .setValue(Value.newBuilder().setStringValue(Messages.toText(aggregateId)))).build());
        }

        if (filters.isEmpty()) {
            return Filter.getDefaultInstance();
        }

        if (filters.size() == 1) {
            return filters.get(0);
        }

        final CompositeFilter.Builder compositeFilter = CompositeFilter.newBuilder();
        compositeFilter.addAllFilters(filters);
        return Filter.newBuilder().setCompositeFilter(compositeFilter).build();
    }

    private static Filter convertContextFieldFilter(EventFilterOrBuilder eventFilter) {
        final List<FieldFilter> contextFieldFilters = eventFilter.getContextFieldFilterList();

        if (contextFieldFilters.isEmpty()) {
            return Filter.getDefaultInstance();
        }

        final CompositeFilter.Builder resultFilter = CompositeFilter.newBuilder();
        resultFilter.setOp(CompositeFilter.Operator.AND);

        for (FieldFilter fieldFilter : contextFieldFilters) {
            resultFilter.addFilters(convertFieldFilter(fieldFilter, CONTEXT_FIELD_TO_PROPERTY));
        }

        return Filter.newBuilder().setCompositeFilter(resultFilter).build();
    }

    private static Filter convertEventFieldFilter(EventFilterOrBuilder eventFilter) {
        final List<FieldFilter> eventFieldFilters = eventFilter.getEventFieldFilterList();

        if (eventFieldFilters.isEmpty()) {
            return Filter.getDefaultInstance();
        }

        final CompositeFilter.Builder resultFilter = CompositeFilter.newBuilder();
        resultFilter.setOp(CompositeFilter.Operator.AND);

        for (FieldFilter fieldFilter : eventFieldFilters) {
            resultFilter.addFilters(convertFieldFilter(fieldFilter, EVENT_FIELD_TO_PROPERTY));
        }

        return Filter.getDefaultInstance();
    }

    private static Filter convertEventTypeFilter(EventFilterOrBuilder eventFilter) {
        final String eventType = eventFilter.getEventType();

        if (eventType.isEmpty()) {
            return Filter.getDefaultInstance();
        }

        final Filter.Builder filter = Filter.newBuilder();
        filter.setPropertyFilter(PropertyFilter.newBuilder()
                .setProperty(PropertyReference.newBuilder().setName(EVENT_TYPE_PROPERTY_NAME))
                .setOp(EQUAL)
                .setValue(Value.newBuilder().setStringValue(eventType)));

        return filter.build();
    }

    private static Filter.Builder convertFieldFilter(FieldFilterOrBuilder fieldFilter,
                                                     Function<FieldMask, PropertyReference>
                                                             conversionFunction) {
        final FieldMask field = fieldFilter.getField();
        final List<Any> values = fieldFilter.getValueList();

        final CompositeFilter.Builder filter = CompositeFilter.newBuilder();
        for (Any value : values) {
            final PropertyFilter valueFilter = PropertyFilter.newBuilder()
                    .setOp(EQUAL)
                    .setProperty(conversionFunction.apply(field))
                    .setValue(makeValue(Messages.toText(value))).build();
            filter.addFilters(Filter.newBuilder().setPropertyFilter(valueFilter));
        }

        return Filter.newBuilder().setCompositeFilter(filter);
    }

    @Nullable
    private static Filter.Builder makeFilter(TimestampOrBuilder timestamp,
                                             PropertyFilter.Operator operator) {
        Filter.Builder filter = null;
        if (!timestamp.equals(Timestamp.getDefaultInstance())) {
            filter = Filter.newBuilder().setPropertyFilter(
                    PropertyFilter.newBuilder()
                            .setOp(operator)
                            .setProperty(PropertyReference
                                    .newBuilder().setName(TIMESTAMP_NANOS_PROPERTY_NAME))
                            .setValue(makeValue(Timestamps.convertToNanos(timestamp)))
                            .build());
        }
        return filter;
    }

    private static final Function<FieldMask, PropertyReference> EVENT_FIELD_TO_PROPERTY = new Function<FieldMask, PropertyReference>() {
        @Nullable
        @Override
        public PropertyReference apply(@Nullable FieldMask fieldMask) {
            if (fieldMask == null) {
                return null;
            }
            return DatastoreProperties.makeEventFieldPropertyReference(fieldMask);
        }
    };

    private static final Function<FieldMask, PropertyReference> CONTEXT_FIELD_TO_PROPERTY = new Function<FieldMask, PropertyReference>() {
        @Nullable
        @Override
        public PropertyReference apply(@Nullable FieldMask fieldMask) {
            if (fieldMask == null) {
                return null;
            }
            return DatastoreProperties.makeContextFieldPropertyReference(fieldMask);
        }
    };
}
