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

package io.spine.server.storage.datastore;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.FieldMask;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.stand.AggregateStateId;
import io.spine.server.stand.StandStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.type.TypeUrl;

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

    private final DsStandStorageDelegate recordStorage;

    public DsStandStorage(DsStandStorageDelegate recordStorage, boolean multitenant) {
        super(multitenant);
        this.recordStorage = recordStorage;
    }

    @Override
    public Iterator<EntityRecord> readAllByType(TypeUrl type) {
        final Iterator<EntityRecord> records = recordStorage.readAllByType(type);
        return records;
    }

    @Override
    public Iterator<EntityRecord> readAllByType(TypeUrl type, FieldMask fieldMask) {
        final Iterator<EntityRecord> records = recordStorage.readAllByType(type, fieldMask);
        return records;
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
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<AggregateStateId> ids) {
        return recordStorage.readMultiple(ids);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<AggregateStateId> ids,
                                                         FieldMask fieldMask) {
        return recordStorage.readMultiple(ids, fieldMask);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords() {
        return recordStorage.readAll();
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(FieldMask fieldMask) {
        return recordStorage.readAll(fieldMask);
    }

    @Override
    protected void writeRecord(AggregateStateId id, EntityRecordWithColumns record) {
        recordStorage.write(id, record);
    }

    @Override
    protected void writeRecords(Map<AggregateStateId, EntityRecordWithColumns> records) {
        recordStorage.writeRecords(records);
    }

    @SuppressWarnings("unused") // Part of API
    protected RecordStorage<AggregateStateId> getRecordStorage() {
        return recordStorage;
    }

    @Override
    public void close() {
        super.close();
        recordStorage.close();
    }
}
