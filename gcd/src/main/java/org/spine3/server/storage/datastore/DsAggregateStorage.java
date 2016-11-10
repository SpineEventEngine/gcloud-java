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
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.Lists;
import com.google.protobuf.Int32Value;
import org.spine3.base.Identifiers;
import org.spine3.protobuf.Timestamps;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageRecord;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Identifiers.idToString;

/**
 * A storage of aggregate root events and snapshots based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @author Dmytro Dashenkov
 * @see DatastoreStorageFactory
 * @see LocalDatastoreStorageFactory
 */
class DsAggregateStorage<I> extends AggregateStorage<I> {

    private static final String AGGREGATE_ID_PROPERTY_NAME = "aggregateId";
    private static final String EVENTS_AFTER_LAST_SNAPSHOT_PREFIX = "EVENTS_AFTER_SNAPSHOT_";
    private static final String SNAPSHOT = "SNAPSHOT";

    private static final String KIND = AggregateStorageRecord.class.getName();
    private static final TypeUrl TYPE_URL = TypeUrl.of(AggregateStorageRecord.getDescriptor());

    private final DatastoreWrapper datastore;
    private final DsPropertyStorage propertyStorage;

    /* package */ static <I> DsAggregateStorage<I> newInstance(
            DatastoreWrapper datastore,
            DsPropertyStorage propertyStorage,
            boolean multitenant) {
        return new DsAggregateStorage<>(datastore, propertyStorage, multitenant);
    }

    private DsAggregateStorage(DatastoreWrapper datastore, DsPropertyStorage propertyStorage, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
        this.propertyStorage = propertyStorage;
    }

    @Override
    public int readEventCountAfterLastSnapshot(I id) {
        checkNotClosed();
        checkNotNull(id);

        final String datastoreId = generateDatastoreId(id);
        final Int32Value count = propertyStorage.read(datastoreId);
        if (count == null) {
            return 0;
        }
        final int countValue = count.getValue();
        return countValue;
    }

    @Override
    public void writeEventCountAfterLastSnapshot(I id, int eventCount) {
        checkNotClosed();
        checkNotNull(id);

        final String datastoreId = generateDatastoreId(id);
        propertyStorage.write(datastoreId, Int32Value.newBuilder()
                                                     .setValue(eventCount)
                                                     .build());
    }

    @Override
    protected void writeRecord(I id, AggregateStorageRecord record) {
        checkNotNull(id);

        final String stringId = idToString(id);
        final KeyFactory keyFactory = datastore.getKeyFactory(KIND);
        String eventId = record.getEventId();
        if (eventId.isEmpty()) {
            // Snapshots have no Event IDs.
            eventId = SNAPSHOT + stringId;
        }
        final Key key = keyFactory.newKey(eventId);
        final Entity incompleteEntity = Entities.messageToEntity(record, key);
        final Entity.Builder builder = Entity.builder(incompleteEntity);
        builder.set(AGGREGATE_ID_PROPERTY_NAME, stringId);
        datastore.createOrUpdate(builder.build());
    }

    @Override
    protected Iterator<AggregateStorageRecord> historyBackward(I id) {
        checkNotNull(id);

        final String idString = idToString(id);
        final Query<?> query = Query.newEntityQueryBuilder()
                                    .setKind(KIND)
                                    .setFilter(StructuredQuery.PropertyFilter.eq(AGGREGATE_ID_PROPERTY_NAME, idString))
                                    .build();
        final List<Entity> eventEntities = datastore.read(query);
        if (eventEntities.isEmpty()) {
            return Collections.emptyIterator();
        }

        final List<AggregateStorageRecord> immutableResult = Entities.entitiesToMessages(eventEntities, TYPE_URL);
        final List<AggregateStorageRecord> records = Lists.newArrayList(immutableResult);
        Collections.sort(records, new Comparator<AggregateStorageRecord>() {
            @Override
            public int compare(AggregateStorageRecord o1, AggregateStorageRecord o2) {
                return Timestamps.compare(o2.getTimestamp(), o1.getTimestamp());
            }
        });
        return records.iterator();
    }

    private String generateDatastoreId(I id) {
        final String stringId = Identifiers.idToString(id);
        final String datastoreid = EVENTS_AFTER_LAST_SNAPSHOT_PREFIX + stringId;
        return datastoreid;
    }
}
