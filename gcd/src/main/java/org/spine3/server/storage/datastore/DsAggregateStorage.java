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
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Message;
import org.spine3.protobuf.Timestamps2;
import org.spine3.server.aggregate.AggregateEventRecord;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.entity.LifecycleFlags;
import org.spine3.type.TypeName;
import org.spine3.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.not;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.aggregate.storage.AggregateField.aggregate_id;
import static org.spine3.server.storage.datastore.DsProperties.activeEntityPredicate;
import static org.spine3.server.storage.datastore.DsProperties.addArchivedProperty;
import static org.spine3.server.storage.datastore.DsProperties.addDeletedProperty;
import static org.spine3.server.storage.datastore.DsProperties.isArchived;
import static org.spine3.server.storage.datastore.DsProperties.isDeleted;

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
    private static final String AGGREGATE_TYPE = "aggregate_type";

    /**
     * Prefix for the string IDs of the {@link AggregateEventRecord records} which represent an aggregate snapshot,
     * not an event.
     *
     * The aggregate snapshots are stored under an ID composed from {@code SNAPSHOT} and the aggregate ID.
     */
    private static final String SNAPSHOT = "SNAPSHOT";

    private static final TypeName AGGREGATE_RECORD_KIND = TypeName.from(AggregateEventRecord.getDescriptor());
    private static final TypeName AGGREGATE_LIFECYCLE_KIND = TypeName.from(LifecycleFlags.getDescriptor());
    private static final TypeUrl AGGREGATE_RECORD_TYPE_URL = TypeUrl.from(AggregateEventRecord.getDescriptor());

    private final DatastoreWrapper datastore;
    private final DsPropertyStorage propertyStorage;
    private final Class<I> idClass;
    private final TypeName stateTypeName;

    public DsAggregateStorage(DatastoreWrapper datastore,
                              DsPropertyStorage propertyStorage,
                              boolean multitenant,
                              Class<I> idClass,
                              Class<? extends Message> stateClass) {
        super(multitenant);
        this.datastore = datastore;
        this.propertyStorage = propertyStorage;
        this.idClass = idClass;
        this.stateTypeName = TypeName.of(stateClass);
    }

    @Override
    public int readEventCountAfterLastSnapshot(I id) {
        checkNotClosed();
        checkNotNull(id);

        final DatastoreRecordId datastoreId = generateDatastoreId(id);
        final Optional<Int32Value> count = propertyStorage.read(datastoreId, Int32Value.getDescriptor());
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
    protected void writeRecord(I id, AggregateEventRecord record) {
        checkNotNull(id);

        final String stringId = idToString(id);
        String eventId = idToString(record.getEvent()
                                          .getContext()
                                          .getEventId());
        if (eventId.isEmpty()) {
            // Snapshots have no Event IDs.
            eventId = SNAPSHOT + stringId;
        }

        final Key key = DsIdentifiers.keyFor(datastore,
                                             AGGREGATE_RECORD_KIND.value(),
                                             DsIdentifiers.of(eventId));
        final Entity incompleteEntity = Entities.messageToEntity(record, key);
        final Entity.Builder builder = Entity.newBuilder(incompleteEntity);
        DsProperties.addAggregateIdProperty(stringId, builder);
        builder.set(AGGREGATE_TYPE, stateTypeName.value());
        datastore.createOrUpdate(builder.build());
    }

    @Override
    protected Iterator<AggregateEventRecord> historyBackward(I id) {
        checkNotNull(id);

        final String idString = idToString(id);
        final Query<Entity> query = Query.newEntityQueryBuilder()
                                         .setKind(AGGREGATE_RECORD_KIND.value())
                                         .setFilter(StructuredQuery.PropertyFilter.eq(aggregate_id.toString(), idString))
                                         .build();
        final List<Entity> eventEntities = datastore.read(query);
        if (eventEntities.isEmpty()) {
            return Collections.emptyIterator();
        }

        final Collection<Entity> aggregateEntityStates = Collections2.filter(
                getEntityStates(),
                not(activeEntityPredicate()));
        final Collection<Key> inactiveAggregateKeys = Collections2.transform(
                aggregateEntityStates,
                new Function<Entity, Key>() {
                    @Nullable
                    @Override
                    public Key apply(@Nullable Entity input) {
                        checkNotNull(input);
                        return input.getKey();
                    }
                });

        final Collection<Entity> filteredEntities = Collections2.filter(eventEntities,
                                                                        new IsActiveAggregateId(inactiveAggregateKeys));
        final List<AggregateEventRecord> immutableResult = Entities.entitiesToMessages(filteredEntities, AGGREGATE_RECORD_TYPE_URL);
        final List<AggregateEventRecord> records = Lists.newArrayList(immutableResult);

        Collections.sort(records, new Comparator<AggregateEventRecord>() {
            @Override
            public int compare(AggregateEventRecord o1, AggregateEventRecord o2) {
                return Timestamps2.compare(o2.getTimestamp(), o1.getTimestamp());
            }
        });
        return records.iterator();
    }

    private Collection<Entity> getEntityStates() {
        final Query<Entity> query = Query.newEntityQueryBuilder()
                                         .setKind(AGGREGATE_LIFECYCLE_KIND.value())
                                         .build();
        return datastore.read(query);
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
        return DsIdentifiers.of(datastoreId);
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
    protected DsPropertyStorage getPropertyStorage() {
        return propertyStorage;
    }

    @Override
    public Optional<LifecycleFlags> readLifecycleFlags(I id) {
        checkNotNull(id);

        final Key key = keyFor(id);
        final Entity entityStateRecord = datastore.read(key);
        if (entityStateRecord == null) {
            return Optional.absent();
        }

        final boolean archived = isArchived(entityStateRecord);
        final boolean deleted = isDeleted(entityStateRecord);

        if (!archived && !deleted) {
            return Optional.absent();
        }
        final LifecycleFlags flags = LifecycleFlags.newBuilder()
                                                   .setArchived(archived)
                                                   .setDeleted(deleted)
                                                   .build();
        return Optional.of(flags);
    }

    @Override
    public void writeLifecycleFlags(I id, LifecycleFlags flags) {
        checkNotNull(id);
        checkNotNull(flags);

        final Key key = keyFor(id);
        final Entity.Builder entityStateRecord = Entity.newBuilder(key);
        addArchivedProperty(entityStateRecord, flags.getArchived());
        addDeletedProperty(entityStateRecord, flags.getDeleted());
        datastore.createOrUpdate(entityStateRecord.build());
    }

    @Override
    public Iterator<I> index() {
        checkNotClosed();
        final StructuredQuery.Filter filter = StructuredQuery.PropertyFilter.eq(AGGREGATE_TYPE, stateTypeName.value());
        return Indexes.indexIterator(datastore, AGGREGATE_RECORD_KIND.value(), idClass, filter);
    }

    private Key keyFor(I id) {
        final DatastoreRecordId recordId = generateDatastoreId(id);
        final Key key = DsIdentifiers.keyFor(datastore, AGGREGATE_LIFECYCLE_KIND.value(), recordId);
        return key;
    }

    private static class IsActiveAggregateId implements Predicate<Entity> {

        private final Collection<Key> inActiveAggregateIds;

        private IsActiveAggregateId(Collection<Key> inActiveAggregateIds) {
            this.inActiveAggregateIds = checkNotNull(inActiveAggregateIds);
        }

        @Override
        public boolean apply(@Nullable Entity input) {
            checkNotNull(input);
            return !inActiveAggregateIds.contains(input.getKey());
        }
    }
}
