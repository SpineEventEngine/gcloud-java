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
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.ColumnTypeRegistry;
import org.spine3.server.event.EventEntity;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.datastore.DsRecordStorage.IdRecordPair;
import org.spine3.server.storage.datastore.type.DatastoreColumnType;
import org.spine3.server.storage.datastore.type.DatastoreTypeRegistryFactory;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.lt;

/**
 * {@inheritDoc}
 */
@Deprecated
class DsEventStorage extends EventStorage {

    protected DsEventStorage(DsRecordStorage<EventId> storage) {
        super(storage);
    }

    @Override
    protected Map<EventId, EntityRecord> readRecords(EventStreamQuery query) {
        final StructuredQuery<Entity> entityQuery = buildQuery(query);
        final DatastoreWrapper datastore = getDelegateStorage().getDatastore();
        final List<Entity> readEntities = datastore.read(entityQuery);
        if (readEntities.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<EventId, EntityRecord> result = processResults(readEntities);
        return result;
    }

    private static StructuredQuery<Entity> buildQuery(EventStreamQuery sourceQuery) {
        final Kind kind = Kind.of(TypeName.of(Event.class));
        final Filter filter = buildFilter(sourceQuery);
        final StructuredQuery.Builder<Entity> entityQuery = Query.newEntityQueryBuilder()
                                                                 .setKind(kind.getValue());
        if (filter == null) {
            return entityQuery.build();
        } else {
            return entityQuery.setFilter(filter)
                              .build();
        }
    }

    @Nullable
    private static Filter buildFilter(EventStreamQuery query) {
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

    private static Filter afterFilter(Timestamp after, DatastoreColumnType<Timestamp, DateTime> columnType) {
        final Value<?> value = columnType.toValue(columnType.convertColumnValue(after));
        final Filter startTimeFilter = gt(EventEntity.CREATED_TIME_COLUMN, value);
        return startTimeFilter;
    }

    private static Filter beforeFilter(Timestamp before, DatastoreColumnType<Timestamp, DateTime> columnType) {
        final Value<?> value = columnType.toValue(columnType.convertColumnValue(before));
        final Filter startTimeFilter = lt(EventEntity.CREATED_TIME_COLUMN, value);
        return startTimeFilter;
    }

    private static DatastoreColumnType<Timestamp, DateTime> getTimestampColumnType() {
        final Class<EventEntity> eventEntityClass = EventEntity.class;
        final Method getter;
        try {
            getter = eventEntityClass.getMethod("getCreated");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        final Column<Timestamp> column = Column.from(getter);
        final ColumnTypeRegistry registry = DatastoreTypeRegistryFactory.defaultInstance();

        @SuppressWarnings("unchecked")
        final DatastoreColumnType<Timestamp, DateTime> timestampType =
                (DatastoreColumnType<Timestamp, DateTime>) registry.get(column);
        return timestampType;
    }

    private Map<EventId, EntityRecord> processResults(List<Entity> entities) {
        final ImmutableMap.Builder<EventId, EntityRecord> records = new ImmutableMap.Builder<>();
        for (Entity entity : entities) {
            final IdRecordPair<EventId> recordPair = getDelegateStorage().getRecordFromEntity(entity);
            records.put(recordPair.getId(), recordPair.getRecord());
        }
        return records.build();
    }

    @Override
    protected DsRecordStorage<EventId> getDelegateStorage() {
        return (DsRecordStorage<EventId>) super.getDelegateStorage();
    }
}
