/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.core.Version;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateEventRecord.KindCase;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.Snapshot;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.entity.model.EntityClass;
import io.spine.string.Stringifiers;
import io.spine.type.TypeName;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Streams.stream;
import static io.spine.server.aggregate.AggregateField.aggregate_id;
import static io.spine.server.entity.model.EntityClass.asEntityClass;
import static io.spine.server.storage.datastore.DatastoreWrapper.MAX_ENTITIES_PER_WRITE_REQUEST;
import static io.spine.server.storage.datastore.DsProperties.addAggregateId;
import static io.spine.server.storage.datastore.DsProperties.addVersion;
import static io.spine.server.storage.datastore.DsProperties.addWhenCreated;
import static io.spine.server.storage.datastore.DsProperties.byCreatedTime;
import static io.spine.server.storage.datastore.DsProperties.byRecordType;
import static io.spine.server.storage.datastore.DsProperties.byVersion;
import static io.spine.server.storage.datastore.DsProperties.isArchived;
import static io.spine.server.storage.datastore.DsProperties.isDeleted;
import static io.spine.server.storage.datastore.DsProperties.isSnapshot;
import static io.spine.server.storage.datastore.DsProperties.markAsArchived;
import static io.spine.server.storage.datastore.DsProperties.markAsDeleted;
import static io.spine.server.storage.datastore.DsProperties.markAsSnapshot;
import static io.spine.server.storage.datastore.DsProperties.whenCreated;
import static io.spine.server.storage.datastore.Entities.fromMessage;
import static io.spine.server.storage.datastore.Entities.toMessage;
import static io.spine.util.Exceptions.newIllegalArgumentException;
import static java.lang.String.format;

/**
 * A storage of aggregate root events and snapshots based on Google Cloud Datastore.
 *
 * @see DatastoreStorageFactory
 */
public class DsAggregateStorage<I> extends AggregateStorage<I> {

    /**
     * Prefix for the record containing the {@link LifecycleFlags} of the entity.
     */
    private static final String LIFECYCLE_RECORD_ID_PREFIX = "LIFECYCLE_FLAGS";

    /**
     * Prefix for the string IDs of the {@link AggregateEventRecord records} which represent
     * an aggregate snapshot, not an event.
     *
     * <p>The aggregate snapshots are stored under an ID composed from {@code SNAPSHOT}, the
     * aggregate ID and the snapshot's {@linkplain com.google.protobuf.Timestamp timestamp}.
     */
    private static final String SNAPSHOT = "SNAPSHOT";

    private static final TypeName AGGREGATE_LIFECYCLE_KIND =
            TypeName.from(LifecycleFlags.getDescriptor());
    private static final TypeUrl AGGREGATE_RECORD_TYPE_URL =
            TypeUrl.from(AggregateEventRecord.getDescriptor());

    private final DatastoreWrapper datastore;
    private final DsPropertyStorage propertyStorage;
    private final Class<I> idClass;
    private final TypeName stateTypeName;

    protected DsAggregateStorage(Class<? extends Aggregate<I, ?, ?>> cls,
                                 DatastoreWrapper datastore,
                                 DsPropertyStorage propertyStorage,
                                 boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;
        this.propertyStorage = propertyStorage;

        EntityClass<? extends Aggregate<I, ?, ?>> modelClass = asEntityClass(cls);
        @SuppressWarnings("unchecked") // The ID class is ensured by the parameter type.
                Class<I> idClass = (Class<I>) modelClass.idClass();
        this.idClass = idClass;
        this.stateTypeName = modelClass.stateType()
                                       .toTypeName();
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // Only valuable cases.
    @Override
    protected void writeRecord(I id, AggregateEventRecord record) {
        checkNotNull(id);

        RecordId recordId;
        Version version;
        KindCase kind = record.getKindCase();
        switch (kind) {
            case EVENT:
                String eventIdString = Stringifiers.toString(record.getEvent()
                                                                   .getId());
                recordId = RecordId.of(eventIdString);
                version = record.getEvent()
                                .getContext()
                                .getVersion();
                break;
            case SNAPSHOT:
                Snapshot snapshot = record.getSnapshot();
                recordId = toSnapshotId(id, snapshot);
                version = snapshot.getVersion();
                break;
            default:
                throw newIllegalArgumentException("Invalid kind of AggregateEventRecord \"%s\".",
                                                  record.getKindCase());
        }
        Key key = datastore.keyFor(Kind.of(stateTypeName), recordId);
        Entity incompleteEntity = fromMessage(record, key);
        Entity.Builder builder = Entity.newBuilder(incompleteEntity);
        addAggregateId(builder, Stringifiers.toString(id));
        addWhenCreated(builder, record.getTimestamp());
        addVersion(builder, version);
        markAsSnapshot(builder, kind == KindCase.SNAPSHOT);
        datastore.createOrUpdate(builder.build());
    }

    /**
     * {@inheritDoc}
     *
     * <p>The resulting iterator will fetch {@linkplain AggregateEventRecord events} batch by batch.
     *
     * <p>Size of the batch is specified by the given {@link AggregateReadRequest}.
     *
     * @param request
     *         the read request
     * @return a new iterator instance
     */
    @Override
    protected Iterator<AggregateEventRecord> historyBackward(AggregateReadRequest<I> request) {
        StructuredQuery<Entity> query = historyBackwardQuery(request);
        Function<Entity, AggregateEventRecord> toRecords = toMessage(AGGREGATE_RECORD_TYPE_URL);
        int batchSize = request.batchSize();
        Iterator<AggregateEventRecord> result =
                stream(datastore.readAll(query, batchSize))
                        .map(toRecords)
                        .iterator();
        return result;
    }

    @Override
    protected void truncate(int snapshotIndex) {
        truncate(snapshotIndex, entity -> true);
    }

    @Override
    protected void truncate(int snapshotIndex, Timestamp date) {
        Predicate<Entity> predicate = entity -> Timestamps.compare(date, whenCreated(entity)) > 0;
        truncate(snapshotIndex, predicate);
    }

    /**
     * Drops the aggregate event records that are older than the Nth snapshot and match the
     * specified predicate.
     *
     * <p>The record deletion happens batch-by-batch where the batch size is the
     * {@link DatastoreWrapper#MAX_ENTITIES_PER_WRITE_REQUEST maximum number} of entities for the
     * Datastore write request.
     */
    private void truncate(int snapshotIndex, Predicate<Entity> predicate) {
        Iterator<Entity> records = readAllForTenant();
        List<Entity> currentBatch = newLinkedList();
        Map<String, Integer> snapshotHitsByAggregateId = newHashMap();
        while (records.hasNext()) {
            Entity record = records.next();
            String id = record.getString(aggregate_id.toString());
            int snapshotsHit = snapshotHitsByAggregateId.get(id) != null
                               ? snapshotHitsByAggregateId.get(id)
                               : 0;
            if (snapshotsHit > snapshotIndex && predicate.test(record)) {
                currentBatch.add(record);
                if (currentBatch.size() == MAX_ENTITIES_PER_WRITE_REQUEST) {
                    datastore.deleteEntities(currentBatch);
                    currentBatch.clear();
                }
            }
            if (isSnapshot(record)) {
                snapshotHitsByAggregateId.put(id, snapshotsHit + 1);
            }
        }
        datastore.deleteEntities(currentBatch);
    }

    @VisibleForTesting
    EntityQuery historyBackwardQuery(AggregateReadRequest<I> request) {
        checkNotNull(request);
        String idString = Stringifiers.toString(request.recordId());
        return historyBackwardQuery()
                .setFilter(eq(aggregate_id.toString(), idString))
                .build();
    }

    private EntityQuery.Builder historyBackwardQuery() {
        return Query.newEntityQueryBuilder()
                    .setKind(stateTypeName.value())
                    .setOrderBy(byVersion(),
                                byCreatedTime(),
                                byRecordType());
    }

    protected RecordId toLifecycleRecordId(I id) {
        String prefix = format("%s_%s", LIFECYCLE_RECORD_ID_PREFIX, stateTypeName);
        return RecordId.withPrefix(id, prefix);
    }

    /**
     * Generates an identifier for the Datastore record which represents an Aggregate snapshot.
     *
     * @param id
     *         an identifier of the {@code Aggregate}
     * @return the identifier for the Datastore record
     */
    protected RecordId toSnapshotId(I id, Snapshot snapshot) {
        String stringId = Stringifiers.toString(id);
        String snapshotTimeStamp = Timestamps.toString(snapshot.getTimestamp());
        String snapshotId = format("%s_%s_%s", SNAPSHOT, stringId, snapshotTimeStamp);
        return RecordId.of(snapshotId);
    }

    /**
     * Provides an access to the GAE Datastore with an API, specific to the Spine framework.
     *
     * <p>Allows the customization of the storage behavior in descendants.
     *
     * @return the wrapped instance of Datastore
     */
    protected DatastoreWrapper datastore() {
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

        Key key = toLifecycleRecordKey(id);
        Entity entityStateRecord = datastore.read(key);
        if (entityStateRecord == null) {
            return Optional.empty();
        }
        boolean archived = isArchived(entityStateRecord);
        boolean deleted = isDeleted(entityStateRecord);

        if (!archived && !deleted) {
            return Optional.empty();
        }
        LifecycleFlags flags = LifecycleFlags
                .newBuilder()
                .setArchived(archived)
                .setDeleted(deleted)
                .vBuild();
        return Optional.of(flags);
    }

    @Override
    public void writeLifecycleFlags(I id, LifecycleFlags flags) {
        checkNotNull(id);
        checkNotNull(flags);

        Key key = toLifecycleRecordKey(id);
        Entity.Builder entityStateRecord = Entity.newBuilder(key);
        markAsArchived(entityStateRecord, flags.getArchived());
        markAsDeleted(entityStateRecord, flags.getDeleted());
        datastore.createOrUpdate(entityStateRecord.build());
    }

    @Override
    public Iterator<I> index() {
        checkNotClosed();

        StructuredQuery<Entity> allQuery = Query.newEntityQueryBuilder()
                                                .setKind(stateTypeName.value())
                                                .build();
        Iterator<I> index = stream(datastore.readAll(allQuery))
                .map(new IndexTransformer<>(idClass))
                .iterator();
        return index;
    }

    private Iterator<Entity> readAllForTenant() {
        EntityQuery query = historyBackwardQuery().build();
        Iterator<Entity> result = datastore.read(query);
        return result;
    }

    private Key toLifecycleRecordKey(I id) {
        RecordId recordId = toLifecycleRecordId(id);
        Key key = datastore.keyFor(Kind.of(AGGREGATE_LIFECYCLE_KIND), recordId);
        return key;
    }

    /**
     * A {@linkplain Function} type transforming String IDs into the specified generic type.
     *
     * @param <I>
     *         the generic ID type
     */
    private static class IndexTransformer<I> implements Function<Entity, I> {

        private final Class<I> idClass;

        private IndexTransformer(Class<I> idClass) {
            this.idClass = idClass;
        }

        @Override
        public I apply(@Nullable Entity entity) {
            checkNotNull(entity);
            String stringId = entity.getString(aggregate_id.toString());
            return Stringifiers.fromString(stringId, idClass);
        }
    }
}
