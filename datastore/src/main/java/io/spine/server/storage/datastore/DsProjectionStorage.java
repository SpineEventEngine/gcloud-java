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
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.validate.Validate;

import javax.annotation.Nullable;
import java.util.Iterator;


/**
 * GAE Datastore implementation of the {@link ProjectionStorage}.
 *
 * @author Dmytro Dashenkov
 * @author Mikhail Mikhaylov
 */
@SuppressWarnings("WeakerAccess")   // Part of API
public class DsProjectionStorage<I> extends ProjectionStorage<I> {

    private static final String LAST_EVENT_TIMESTAMP_ID = "datastore_event_timestamp_";

    private final DsRecordStorage<I> recordStorage;
    private final DsPropertyStorage propertyStorage;

    private final RecordId lastTimestampId;

    public DsProjectionStorage(DsRecordStorage<I> recordStorage,
                               DsPropertyStorage propertyStorage,
                               Class<? extends Entity<I, ?>> projectionClass,
                               boolean multitenant) {
        super(multitenant);
        this.recordStorage = recordStorage;
        this.propertyStorage = propertyStorage;
        this.lastTimestampId =
                DsIdentifiers.of(LAST_EVENT_TIMESTAMP_ID + projectionClass.getCanonicalName());
    }

    @Override
    public void writeLastHandledEventTime(Timestamp timestamp) {
        propertyStorage.write(lastTimestampId, timestamp);
    }

    @Nullable
    @Override
    public Timestamp readLastHandledEventTime() {
        final Optional<Timestamp> readTimestamp = propertyStorage.read(lastTimestampId,
                                                                       Timestamp.getDescriptor());

        if ((!readTimestamp.isPresent()) || Validate.isDefault(readTimestamp.get())) {
            return null;
        }
        final Timestamp result = readTimestamp.get();
        return result;
    }

    @Override
    public RecordStorage<I> recordStorage() {
        return recordStorage;
    }

    protected DsPropertyStorage propertyStorage() {
        return propertyStorage;
    }

    @Override
    public boolean delete(I id) {
        return recordStorage.delete(id);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids) {
        return recordStorage().readMultiple(ids);
    }

    @Override
    protected Iterator<EntityRecord> readMultipleRecords(Iterable<I> ids, FieldMask fieldMask) {
        return recordStorage().readMultiple(ids, fieldMask);
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords() {
        return recordStorage().readAll();
    }

    @Override
    protected Iterator<EntityRecord> readAllRecords(FieldMask fieldMask) {
        return recordStorage().readAll(fieldMask);
    }

    @Override
    public Iterator<I> index() {
        return recordStorage.index();
    }
}
