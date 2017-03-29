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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.FieldMask;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.storage.EntityRecordWithStorageFields;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.stand.StandStorage;
import org.spine3.server.storage.RecordStorage;
import org.spine3.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

/**
 * Google cloud Datastore implementation of {@link StandStorage}.
 *
 * @author Dmytro Dashenkov
 */

@SuppressWarnings("WeakerAccess")   // Part of API
public class DsStandStorage extends StandStorage {

    private final DsRecordStorage<AggregateStateId> recordStorage;

    public DsStandStorage(DsRecordStorage<AggregateStateId> recordStorage, boolean multitenant) {
        super(multitenant);
        this.recordStorage = recordStorage;
    }

    @Override
    public ImmutableCollection<EntityRecord> readAllByType(TypeUrl type) {
        final Map<?, EntityRecord> records = recordStorage.readAllByType(type);
        final ImmutableList<EntityRecord> result = ImmutableList.copyOf(records.values());
        return result;
    }

    @Override
    public ImmutableCollection<EntityRecord> readAllByType(TypeUrl type, FieldMask fieldMask) {
        final Map<?, EntityRecord> records = recordStorage.readAllByType(type, fieldMask);
        final ImmutableList<EntityRecord> result = ImmutableList.copyOf(records.values());
        return result;
    }

    @Override
    public Iterator<AggregateStateId> index() {
        return recordStorage.index();
    }

    @Override
    public boolean delete(AggregateStateId id) {
        return recordStorage.delete(id);
    }

    @Nullable
    @Override
    protected Optional<EntityRecord> readRecord(AggregateStateId id) {
        return recordStorage.read(id);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<AggregateStateId> ids) {
        return recordStorage.readMultiple(ids);
    }

    @Override
    protected Iterable<EntityRecord> readMultipleRecords(Iterable<AggregateStateId> ids, FieldMask fieldMask) {
        return recordStorage.readMultiple(ids, fieldMask);
    }

    @Override
    protected Map<AggregateStateId, EntityRecord> readAllRecords() {
        return recordStorage.readAll();
    }

    @Override
    protected Map<AggregateStateId, EntityRecord> readAllRecords(FieldMask fieldMask) {
        return recordStorage.readAll(fieldMask);
    }

    @Override
    protected void writeRecord(AggregateStateId id, EntityRecordWithStorageFields record) {
        recordStorage.write(id, record);
    }

    @Override
    protected void writeRecords(Map<AggregateStateId, EntityRecordWithStorageFields> records) {
        recordStorage.writeRecords(records);
    }

    @SuppressWarnings("unused") // Part of API
    protected RecordStorage<AggregateStateId> getRecordStorage() {
        return recordStorage;
    }

    @Override
    public void close() throws Exception {
        super.close();
        recordStorage.close();
    }
}
