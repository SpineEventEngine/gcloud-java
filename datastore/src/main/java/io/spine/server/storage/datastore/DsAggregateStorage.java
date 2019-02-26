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
import com.google.protobuf.Int32Value;
import com.google.protobuf.Message;
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
import java.util.Optional;
import java.util.function.Function;

import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;
import static io.spine.server.aggregate.AggregateField.aggregate_id;
import static io.spine.server.entity.model.EntityClass.asEntityClass;
import static io.spine.server.storage.datastore.DsProperties.addAggregateId;
import static io.spine.server.storage.datastore.DsProperties.addVersion;
import static io.spine.server.storage.datastore.DsProperties.addWhenCreated;
import static io.spine.server.storage.datastore.DsProperties.byCreatedTime;
import static io.spine.server.storage.datastore.DsProperties.byRecordType;
import static io.spine.server.storage.datastore.DsProperties.byVersion;
import static io.spine.server.storage.datastore.DsProperties.isArchived;
import static io.spine.server.storage.datastore.DsProperties.isDeleted;
import static io.spine.server.storage.datastore.DsProperties.markAsArchived;
import static io.spine.server.storage.datastore.DsProperties.markAsDeleted;
import static io.spine.server.storage.datastore.DsProperties.markAsSnapshot;
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

    private static final String EVENTS_AFTER_LAST_SNAPSHOT_PREFIX = "EVENTS_AFTER_SNAPSHOT";

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

    @Override
    public int readEventCountAfterLastSnapshot(I id) {
        checkNotClosed();
        checkNotNull(id);

        RecordId recordId = toEventCountId(id);
        Optional<Int32Value> eventCount = lookUpEventCount(recordId);
        if (!eventCount.isPresent()) {
            return readEventCountOldFormat(id);
        }
        int result = eventCount.get()
                               .getValue();
        return result;
    }

    @Override
    public void writeEventCountAfterLastSnapshot(I id, int eventCount) {
        checkNotClosed();
        checkNotNull(id);

        RecordId datastoreId = toEventCountId(id);
        propertyStorage.write(datastoreId, Int32Value.newBuilder()
                                                     .setValue(eventCount)
                                                     .build());
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
        int batchSize = request.getBatchSize();
        Iterator<AggregateEventRecord> result =
                stream(datastore.readAll(query, batchSize))
                        .map(toRecords)
                        .iterator();
        return result;
    }

    @VisibleForTesting
    EntityQuery historyBackwardQuery(AggregateReadRequest<I> request) {
        checkNotNull(request);
        String idString = Stringifiers.toString(request.getRecordId());
        return Query.newEntityQueryBuilder()
                    .setKind(stateTypeName.value())
                    .setFilter(eq(aggregate_id.toString(),
                                  idString))
                    .setOrderBy(byVersion(),
                                byCreatedTime(),
                                byRecordType())
                    .build();
    }

    /**
     * Generates an identifier of the Datastore record basing on the given {@code Aggregate}
     * identifier.
     *
     * @param id
     *         an identifier of the {@code Aggregate}
     * @return the Datastore record ID
     */
    protected RecordId toRecordId(I id) {
        String stringId = Stringifiers.toString(id);
        String datastoreId = format("%s_%s", EVENTS_AFTER_LAST_SNAPSHOT_PREFIX, stringId);
        return RecordId.of(datastoreId);
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
     * Generates an identifier for the Datastore record which represents an event count after the
     * last snapshot.
     *
     * @param id
     *         an identifier of the {@code Aggregate}
     * @return the identifier for the Datastore record
     */
    protected RecordId toEventCountId(I id) {
        String stringId = Stringifiers.toString(id);
        String datastoreId =
                format("%s_%s_%s", EVENTS_AFTER_LAST_SNAPSHOT_PREFIX, stateTypeName, stringId);
        return RecordId.of(datastoreId);
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

        Key key = toKey(id);
        Entity entityStateRecord = datastore.read(key);
        if (entityStateRecord == null) {
            return Optional.empty();
        }

        boolean archived = isArchived(entityStateRecord);
        boolean deleted = isDeleted(entityStateRecord);

        if (!archived && !deleted) {
            return Optional.empty();
        }
        LifecycleFlags flags = LifecycleFlags.newBuilder()
                                             .setArchived(archived)
                                             .setDeleted(deleted)
                                             .build();
        return Optional.of(flags);
    }

    @Override
    public void writeLifecycleFlags(I id, LifecycleFlags flags) {
        checkNotNull(id);
        checkNotNull(flags);

        Key key = toKey(id);
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

    private Key toKey(I id) {
        RecordId recordId = toRecordId(id);
        Key key = datastore.keyFor(Kind.of(AGGREGATE_LIFECYCLE_KIND), recordId);
        return key;
    }

    /**
     * Tries to look up the event count among the records saved with the old format ID.
     *
     * <p>The method exists so the {@code DsAggregateStorage} is compatible with the data saved
     * pre-ID format change.
     */
    private int readEventCountOldFormat(I id) {
        RecordId oldFormatId = toRecordId(id);
        Optional<Int32Value> eventCount = lookUpEventCount(oldFormatId);
        if (!eventCount.isPresent()) {
            return 0;
        }
        int result = eventCount.get()
                               .getValue();
        return result;
    }

    private Optional<Int32Value> lookUpEventCount(RecordId recordId) {
        Optional<Message> eventCount = propertyStorage.read(recordId, Int32Value.getDescriptor());
        if (!eventCount.isPresent()) {
            return Optional.empty();
        }
        Int32Value count = (Int32Value) eventCount.get();
        return Optional.of(count);
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
