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

import com.google.common.collect.ImmutableCollection;
import com.google.protobuf.FieldMask;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.StandStorage;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * @author Dmytro Dashenkov
 */
/*package*/ class DsStandStorage extends StandStorage {

    // TODO:14-10-16:dmytro.dashenkov: Implement.

    protected DsStandStorage(boolean multitenant) {
        super(multitenant);
    }

    @Override
    public ImmutableCollection<EntityStorageRecord> readAllByType(TypeUrl type) {
        return null;
    }

    @Override
    public ImmutableCollection<EntityStorageRecord> readAllByType(TypeUrl type, FieldMask fieldMask) {
        return null;
    }

    @Nullable
    @Override
    protected EntityStorageRecord readRecord(AggregateStateId id) {
        return null;
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<AggregateStateId> ids) {
        return null;
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<AggregateStateId> ids, FieldMask fieldMask) {
        return null;
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllRecords() {
        return null;
    }

    @Override
    protected Map<AggregateStateId, EntityStorageRecord> readAllRecords(FieldMask fieldMask) {
        return null;
    }

    @Override
    protected void writeRecord(AggregateStateId id, EntityStorageRecord record) {

    }
}
