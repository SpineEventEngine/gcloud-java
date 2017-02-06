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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.stand.StandStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.RecordStorage;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.spine3.server.storage.datastore.DatastoreIdentifiers.of;

/**
 * Google cloud Datastore implementation of {@link StandStorage}.
 *
 * @author Dmytro Dashenkov
 */

@SuppressWarnings("WeakerAccess")   // Part of API
public class DsStandStorage extends StandStorage {

    private final DsRecordStorage<DatastoreRecordId> recordStorage;

    public DsStandStorage(DsRecordStorage<DatastoreRecordId> recordStorage, boolean multitenant) {
        super(multitenant);
        this.recordStorage = recordStorage;
    }

    @Override
    public ImmutableCollection<EntityStorageRecord> readAllByType(TypeUrl type) {
        final Map<?, EntityStorageRecord> records = recordStorage.readAllByType(type);
        final ImmutableList<EntityStorageRecord> result = ImmutableList.copyOf(records.values());
        return result;
    }

    @Override
    public ImmutableCollection<EntityStorageRecord> readAllByType(TypeUrl type, FieldMask fieldMask) {
        final Map<?, EntityStorageRecord> records = recordStorage.readAllByType(type, fieldMask);
        final ImmutableList<EntityStorageRecord> result = ImmutableList.copyOf(records.values());
        return result;
    }

    @Override
    public boolean markArchived(AggregateStateId id) {
        final DatastoreRecordId recordId = of(id);
        return recordStorage.markArchived(recordId);
    }

    @Override
    public boolean markDeleted(AggregateStateId id) {
        final DatastoreRecordId recordId = of(id);
        return recordStorage.markDeleted(recordId);
    }

    @Override
    public boolean delete(AggregateStateId id) {
        final DatastoreRecordId recordId = of(id);
        return recordStorage.delete(recordId);
    }

    @Nullable
    @Override
    protected Optional<EntityStorageRecord> readRecord(AggregateStateId id) {
        final DatastoreRecordId recordId = of(id);
        return recordStorage.read(recordId);
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<AggregateStateId> ids) {
        return recordStorage.readMultiple(transformIds(ids));
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<AggregateStateId> ids, FieldMask fieldMask) {
        final Iterable<DatastoreRecordId> recordIds = transformIds(ids);
        return recordStorage.readMultiple(recordIds, fieldMask);
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllRecords() {
        return readAllRecords(FieldMask.getDefaultInstance());
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllRecords(FieldMask fieldMask) {
        final Map<DatastoreRecordId, EntityStorageRecord> readRecords = recordStorage.readAllRecords(fieldMask);
        final Collection<DatastoreRecordId> sourceIds = readRecords.keySet();
        final Collection<AggregateStateId> ids = Collections2.transform(sourceIds, reverseIdTransformer());

        final Collection<EntityStorageRecord> recordValues = readRecords.values();
        final Iterator<EntityStorageRecord> recordIterator = recordValues.iterator();
        final ImmutableMap.Builder<AggregateStateId, EntityStorageRecord> result = new ImmutableMap.Builder<>();
        for (AggregateStateId id : ids) {
            checkState(recordIterator.hasNext(), "Set of read values is shorter then set of keys.");
            result.put(id, recordIterator.next());
        }
        checkState(!recordIterator.hasNext(), "Set of read values is longer then set of keys.");

        return result.build();
    }

    @Override
    protected void writeRecord(AggregateStateId id, EntityStorageRecord record) {
        final DatastoreRecordId recordId = of(id);
        recordStorage.write(recordId, record);
    }

    @Override
    protected void writeRecords(Map<AggregateStateId, EntityStorageRecord> records) {
        final Map<DatastoreRecordId, EntityStorageRecord> datastoreRecords = new HashMap<>(records.size());
        for (Map.Entry<AggregateStateId, EntityStorageRecord> entry : records.entrySet()) {
            final DatastoreRecordId id = of(entry.getKey());
            datastoreRecords.put(id, entry.getValue());
        }
        recordStorage.writeRecords(datastoreRecords);
    }

    @SuppressWarnings("unused") // Part of API
    protected RecordStorage<DatastoreRecordId> getRecordStorage() {
        return recordStorage;
    }

    private static Iterable<DatastoreRecordId> transformIds(Iterable<AggregateStateId> ids) {
        final Iterable<DatastoreRecordId> recordIds =
                Iterables.transform(ids,
                                    new Function<AggregateStateId, DatastoreRecordId>() {
                                        @Nullable
                                        @Override
                                        public DatastoreRecordId apply(@Nullable AggregateStateId input) {
                                            checkNotNull(input);
                                            return of(input);
                                        }
                                    });
        return recordIds;
    }

    private static Function<Object, AggregateStateId> reverseIdTransformer() {
        return reverseIdTransformer(null);
    }

    private static Function<Object, AggregateStateId> reverseIdTransformer(@Nullable final TypeUrl withType) {
        final TypeUrl type = withType != null ? withType : TypeUrl.of(Any.class);
        return new Function<Object, AggregateStateId>() {
            @Override
            public AggregateStateId apply(@Nullable Object input) {
                checkNotNull(input, "Aggregate ID must not be null.");
                final AggregateStateId id = AggregateStateId.of(input, type);
                return id;
            }
        };
    }
}
