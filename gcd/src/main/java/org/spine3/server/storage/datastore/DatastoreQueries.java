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

import com.google.api.services.datastore.DatastoreV1;
import com.google.api.services.datastore.DatastoreV1.CompositeFilter;
import com.google.api.services.datastore.DatastoreV1.Filter;
import com.google.api.services.datastore.DatastoreV1.PropertyFilter;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;
import org.spine3.base.FieldFilter;
import org.spine3.protobuf.Messages;
import org.spine3.protobuf.Timestamps;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventFilterOrBuilder;
import org.spine3.server.event.EventStreamQueryOrBuilder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.api.services.datastore.DatastoreV1.PropertyFilter.Operator.EQUAL;
import static com.google.api.services.datastore.DatastoreV1.PropertyFilter.Operator.GREATER_THAN;
import static com.google.api.services.datastore.DatastoreV1.PropertyFilter.Operator.LESS_THAN;
import static com.google.api.services.datastore.client.DatastoreHelper.makeOrder;
import static com.google.api.services.datastore.client.DatastoreHelper.makeProperty;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;
import static org.spine3.server.storage.datastore.DatastoreProperties.AGGREGATE_ID_PROPERTY_NAME;
import static org.spine3.server.storage.datastore.DatastoreProperties.EVENT_TYPE_PROPERTY_NAME;
import static org.spine3.server.storage.datastore.DatastoreProperties.TIMESTAMP_NANOS_PROPERTY_NAME;

/**
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
     * @see DatastoreV1.Entity
     * @see com.google.protobuf.Timestamp
     * @see DatastoreV1.Query
     */
    /* package */
    static DatastoreV1.Query.Builder makeQuery(DatastoreV1.PropertyOrder.Direction sortDirection, String entityKind) {
        final DatastoreV1.Query.Builder query = DatastoreV1.Query.newBuilder();
        query.addKindBuilder().setName(entityKind);
        query.addOrder(makeOrder(TIMESTAMP_NANOS_PROPERTY_NAME, sortDirection));
        return query;
    }

    /* package */
    static DatastoreV1.Query.Builder makeQuery(DatastoreV1.PropertyOrder.Direction sortDirection, String entityKind,
                                               EventStreamQueryOrBuilder queryPredicate) {
        final DatastoreV1.Query.Builder query = DatastoreV1.Query.newBuilder();

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
            compositeFilter.setOperator(CompositeFilter.Operator.AND);

            for (Filter filter : filters) {
                compositeFilter.addFilter(filter);
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
                    .setProperty(DatastoreV1.PropertyReference.newBuilder().setName(AGGREGATE_ID_PROPERTY_NAME))
                    .setOperator(EQUAL)
                    .setValue(DatastoreV1.Value.newBuilder().setStringValue(Messages.toText(aggregateId)))).build());
        }

        if (filters.isEmpty()) {
            return Filter.getDefaultInstance();
        }

        if (filters.size() == 1) {
            return filters.get(0);
        }

        final CompositeFilter.Builder compositeFilter = CompositeFilter.newBuilder();
        compositeFilter.addAllFilter(filters);
        return Filter.newBuilder().setCompositeFilter(compositeFilter).build();
    }

    // TODO:2016-04-13:mikhail.mikhaylov: Refactor.
    private static Filter convertContextFieldFilter(EventFilterOrBuilder eventFilter) {
        final List<FieldFilter> contextFieldFilters = eventFilter.getContextFieldFilterList();

        if (contextFieldFilters.isEmpty()) {
            return Filter.getDefaultInstance();
        }

        final CompositeFilter.Builder resultFilter = CompositeFilter.newBuilder();
        resultFilter.setOperator(CompositeFilter.Operator.AND);

        for (FieldFilter fieldFilter : contextFieldFilters) {
            final FieldMask field = fieldFilter.getField();
            final List<Any> values = fieldFilter.getValueList();

            final CompositeFilter.Builder filter = CompositeFilter.newBuilder();
            for (Any value : values) {
                final PropertyFilter valueFilter = PropertyFilter.newBuilder()
                        .setOperator(EQUAL)
                        .setProperty(DatastoreProperties.makeContextFieldPropertyReference(field))
                        .setValue(makeValue(Messages.toText(value))).build();
                filter.addFilter(Filter.newBuilder().setPropertyFilter(valueFilter));
            }

            resultFilter.addFilter(Filter.newBuilder().setCompositeFilter(filter));
        }

        return Filter.newBuilder().setCompositeFilter(resultFilter).build();
    }

    // TODO:2016-04-13:mikhail.mikhaylov: Refactor.
    private static Filter convertEventFieldFilter(EventFilterOrBuilder eventFilter) {
        final List<FieldFilter> eventFieldFilters = eventFilter.getEventFieldFilterList();

        if (eventFieldFilters.isEmpty()) {
            return Filter.getDefaultInstance();
        }

        final CompositeFilter.Builder resultFilter = CompositeFilter.newBuilder();
        resultFilter.setOperator(CompositeFilter.Operator.AND);

        for (FieldFilter fieldFilter : eventFieldFilters) {
            final FieldMask field = fieldFilter.getField();
            final List<Any> values = fieldFilter.getValueList();

            final CompositeFilter.Builder filter = CompositeFilter.newBuilder();
            for (Any value : values) {
                final PropertyFilter valueFilter = PropertyFilter.newBuilder()
                        .setOperator(EQUAL)
                        .setProperty(DatastoreProperties.makeEventFieldPropertyReference(field))
                        .setValue(makeValue(Messages.toText(value))).build();
                filter.addFilter(Filter.newBuilder().setPropertyFilter(valueFilter));
            }

            resultFilter.addFilter(Filter.newBuilder().setCompositeFilter(filter));
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
                .setProperty(DatastoreV1.PropertyReference.newBuilder().setName(EVENT_TYPE_PROPERTY_NAME))
                .setOperator(EQUAL)
                .setValue(DatastoreV1.Value.newBuilder().setStringValue(eventType)));

        return filter.build();
    }

    @Nullable
    private static Filter.Builder makeFilter(TimestampOrBuilder timestamp,
                                             PropertyFilter.Operator operator) {
        Filter.Builder filter = null;
        if (!timestamp.equals(Timestamp.getDefaultInstance())) {
            filter = Filter.newBuilder().setPropertyFilter(
                    PropertyFilter.newBuilder()
                            .setOperator(operator)
                            .setProperty(DatastoreV1.PropertyReference
                                    .newBuilder().setName(TIMESTAMP_NANOS_PROPERTY_NAME))
                            .setValue(makeValue(Timestamps.convertToNanos(timestamp)))
                            .build());
        }
        return filter;
    }
}
