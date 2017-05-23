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

import com.google.cloud.datastore.DateTime;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.cloud.datastore.Value;
import com.google.protobuf.Timestamp;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.ColumnTypeRegistry;
import org.spine3.server.event.EventEntity;
import org.spine3.server.event.EventFilter;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventRecordStorage;
import org.spine3.server.storage.datastore.DsRecordStorage.IdRecordPair;
import org.spine3.server.storage.datastore.type.DatastoreColumnType;
import org.spine3.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import org.spine3.server.storage.datastore.type.SimpleDatastoreColumnType;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.lt;
import static java.util.Collections.singleton;

/**
 * {@inheritDoc}
 */
@Deprecated
class DsEventRecordStorage extends EventRecordStorage {

    protected DsEventRecordStorage(DsRecordStorage<EventId> storage) {
        super(storage);
    }

    @Override
    protected Map<EventId, EntityRecord> readRecords(EventStreamQuery query) {
        final Iterable<StructuredQuery<Entity>> queries = buildQueries(query);
        final DatastoreWrapper datastore = getDelegateStorage().getDatastore();
        final Map<EventId, EntityRecord> result = new HashMap<>();
        for (StructuredQuery<Entity> entityQuery : queries) {
            final List<Entity> readEntities = datastore.read(entityQuery);
            dumpResultsTo(result, readEntities);
        }
        return result;
    }

    private static Iterable<StructuredQuery<Entity>> buildQueries(EventStreamQuery sourceQuery) {
        final Kind kind = Kind.of(TypeName.of(Event.class));
        final Filter timeFilter = buildTimeFilter(sourceQuery);
        final StructuredQuery.Builder<Entity> entityQuery = Query.newEntityQueryBuilder()
                                                                 .setKind(kind.getValue());
        final Collection<Filter> typeFilters = buildTypeFilters(sourceQuery);
        if (typeFilters.isEmpty()) {
            if (timeFilter != null) {
                entityQuery.setFilter(timeFilter);
            }
            return singleton(entityQuery.build());
        } else {
            final Collection<StructuredQuery<Entity>> result = new LinkedList<>();
            final Iterable<Filter> filters = mergeFilters(typeFilters, timeFilter);
            for (Filter filter : filters) {
                final StructuredQuery<Entity> query = entityQuery.setFilter(filter)
                                                                 .build();
                result.add(query);
            }
            return result;
        }
    }

    private static Iterable<Filter> mergeFilters(Iterable<Filter> typeFilters,
                                                 @Nullable Filter timeFilter) {
        if (timeFilter == null) {
            return typeFilters;
        }

        final Collection<Filter> result = new LinkedList<>();
        for (Filter type : typeFilters) {
            result.add(and(timeFilter, type));
        }
        return result;
    }

    @Nullable
    private static Filter buildTimeFilter(EventStreamQuery query) {
        if (!query.hasBefore() && !query.hasAfter()) {
            return null;
        }
        final DatastoreColumnType<Timestamp, DateTime> columnType = getTimestampColumnType();
        Filter after = null;
        if (query.hasAfter()) {
            after = afterFilter(query.getAfter(), columnType);
        }
        Filter before = null;
        if (query.hasBefore()) {
            before = beforeFilter(query.getBefore(), columnType);
        }
        if (after != null && before != null) {
            return and(before, after);
        } else {
            return after == null
                   ? before
                   : after;
        }
    }

    private static Collection<Filter> buildTypeFilters(EventStreamQuery query) {
        final Collection<Filter> result = new LinkedList<>();
        final SimpleDatastoreColumnType<String> columnType = getStringColumnType();
        for (EventFilter filter : query.getFilterList()) {
            final Filter typeFilter = typeFilter(filter.getEventType(), columnType);
            result.add(typeFilter);
        }
        return result;
    }

    private static Filter afterFilter(Timestamp after,
                                      DatastoreColumnType<Timestamp, DateTime> columnType) {
        final Value<?> value = columnType.toValue(columnType.convertColumnValue(after));
        final Filter startTimeFilter = gt(EventEntity.CREATED_TIME_COLUMN, value);
        return startTimeFilter;
    }

    private static Filter beforeFilter(Timestamp before,
                                       DatastoreColumnType<Timestamp, DateTime> columnType) {
        final Value<?> value = columnType.toValue(columnType.convertColumnValue(before));
        final Filter startTimeFilter = lt(EventEntity.CREATED_TIME_COLUMN, value);
        return startTimeFilter;
    }

    private static Filter typeFilter(String type, SimpleDatastoreColumnType<String> columnType) {
        final Value<?> value = columnType.toValue(columnType.convertColumnValue(type));
        final Filter typeFilter = eq(EventEntity.TYPE_COLUMN, value);
        return typeFilter;
    }

    /**
     * Retrieves the default {@link DatastoreColumnType} implementation for {@link Timestamp}.
     *
     * <p>The method uses Entity Column declared with {@link EventEntity#getCreated()} method and
     * the {@link DatastoreTypeRegistryFactory#defaultInstance()} to retrieve the resulting Column
     * Type.
     *
     * <p>This method should only be used to get the Column Type for the column declared with
     * {@link EventEntity#getCreated()} method as a part of {@link EventStreamQuery} processing.
     */
    private static DatastoreColumnType<Timestamp, DateTime> getTimestampColumnType() {
        return getDefaultColumnType(EventEntity.class, "getCreated");
    }

    /**
     * Retrieves the default {@link DatastoreColumnType} implementation for {@link String}.
     *
     * <p>The method uses Entity Column declared with {@link EventEntity#getType()} method and
     * the {@link DatastoreTypeRegistryFactory#defaultInstance()} to retrieve the resulting Column
     * Type.
     *
     * <p>This method should only be used to get the Column Type for the column declared with
     * {@link EventEntity#getType()} method as a part of {@link EventStreamQuery} processing.
     */
    private static SimpleDatastoreColumnType<String> getStringColumnType() {
        return getDefaultColumnType(EventEntity.class, "getType");
    }

    private static <T extends DatastoreColumnType<?, ?>> T getDefaultColumnType(
            Class<? extends org.spine3.server.entity.Entity> cls, String getterName) {
        final Method getter;
        try {
            getter = cls.getMethod(getterName);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        final Column<?> column = Column.from(getter);
        final ColumnTypeRegistry registry = DatastoreTypeRegistryFactory.defaultInstance();
        @SuppressWarnings("unchecked") // Checked for the default column type registry
        final T result = (T) registry.get(column);
        return result;
    }

    private void dumpResultsTo(Map<EventId, EntityRecord> destination, List<Entity> results) {
        for (Entity entity : results) {
            final IdRecordPair<EventId> recordPair =
                    getDelegateStorage().getRecordFromEntity(entity);
            destination.put(recordPair.getId(), recordPair.getRecord());
        }
    }

    @Override
    protected DsRecordStorage<EventId> getDelegateStorage() {
        return (DsRecordStorage<EventId>) super.getDelegateStorage();
    }
}
