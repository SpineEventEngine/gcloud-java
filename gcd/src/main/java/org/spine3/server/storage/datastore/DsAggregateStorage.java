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
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.protobuf.Int32Value;
import org.spine3.protobuf.Timestamps;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.aggregate.storage.AggregateStorageRecord;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Stringifiers.idToString;
import static org.spine3.server.storage.datastore.DatastoreProperties.AGGREGATE_ID_PROPERTY_NAME;

/**
 * A storage of aggregate root events and snapshots based on Google Cloud Datastore.
 *
 * @author Alexander Litus
 * @author Dmytro Dashenkov
 * @see DatastoreStorageFactory
 */
@SuppressWarnings("WeakerAccess")   // Part of API
public class DsAggregateStorage<I> extends AggregateStorage<I> {

    private static final String EVENTS_AFTER_LAST_SNAPSHOT_PREFIX = "EVENTS_AFTER_SNAPSHOT_";
    private static final String SNAPSHOT = "SNAPSHOT";

    private static final String KIND = AggregateStorageRecord.class.getName();
    private static final TypeUrl TYPE_URL = TypeUrl.from(AggregateStorageRecord.getDescriptor());

    private final DatastoreWrapper datastore;
    private final DsPropertyStorage propertyStorage;

    public DsAggregateStorage(DatastoreWrapper datastore, DsPropertyStorage propertyStorage, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
        this.propertyStorage = propertyStorage;
    }

    @Override
    public int readEventCountAfterLastSnapshot(I id) {
        checkNotClosed();
        checkNotNull(id);

        final DatastoreRecordId datastoreId = generateDatastoreId(id);
        final Optional<Int32Value> count = propertyStorage.read(datastoreId);
        final int countValue;
        if (!count.isPresent()) {
            countValue = 0;
        } else {
            countValue = count.get()
                              .getValue();
        }
        return countValue;
    }

    @Override
    public void writeEventCountAfterLastSnapshot(I id, int eventCount) {
        checkNotClosed();
        checkNotNull(id);

        final DatastoreRecordId datastoreId = generateDatastoreId(id);
        propertyStorage.write(datastoreId, Int32Value.newBuilder()
                                                     .setValue(eventCount)
                                                     .build());
    }

    @Override
    protected void writeRecord(I id, AggregateStorageRecord record) {
        checkNotNull(id);

        final String stringId = idToString(id);
        String eventId = record.getEventId();
        if (eventId.isEmpty()) {
            // Snapshots have no Event IDs.
            eventId = SNAPSHOT + stringId;
        }

        final Key key = DatastoreIdentifiers.keyFor(datastore, KIND, DatastoreIdentifiers.of(eventId));
        final Entity incompleteEntity = Entities.messageToEntity(record, key);
        final Entity.Builder builder = Entity.newBuilder(incompleteEntity);
        DatastoreProperties.addAggregateIdProperty(stringId, builder);
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

    /**
     * Generates an identifier of the Datastore record basing on the given {@code Aggregate} identifier.
     *
     * @param id an identifier of the {@code Aggregate}
     * @return the Datastore record ID
     */
    protected DatastoreRecordId generateDatastoreId(I id) {
        final String stringId = idToString(id);
        final String datastoreId = EVENTS_AFTER_LAST_SNAPSHOT_PREFIX + stringId;
        return DatastoreIdentifiers.of(datastoreId);
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

    /**
     * Provides an access to the {@link DsPropertyStorage}.
     *
     * <p>Allows the customization of the storage behavior in descendants.
     *
     * @return the wrapped instance of Datastore
     */
    @SuppressWarnings("unused")     // Part of the API.
    protected DsPropertyStorage getPropertyStorage() {
        return propertyStorage;
    }
}
