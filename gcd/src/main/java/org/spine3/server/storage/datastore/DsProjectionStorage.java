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
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import org.spine3.server.entity.Entity;
import org.spine3.server.projection.ProjectionStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.RecordStorage;
import org.spine3.validate.Validate;

import javax.annotation.Nullable;
import java.util.Map;

import static org.spine3.server.storage.datastore.DatastoreIdentifiers.of;

/**
 * GAE Datastore implementation of the {@link ProjectionStorage}.
 *
 * @author Mikhail Mikhaylov
 */
@SuppressWarnings("WeakerAccess")   // Part of API
public class DsProjectionStorage<I> extends ProjectionStorage<I> {

    private static final String LAST_EVENT_TIMESTAMP_ID = "datastore_event_timestamp_";

    private final DsRecordStorage<I> entityStorage;
    private final DsPropertyStorage propertyStorage;

    private final DatastoreRecordId lastTimestampId;

    public DsProjectionStorage(DsRecordStorage<I> entityStorage,
                               DsPropertyStorage propertyStorage,
                               Class<? extends Entity<I, ?>> projectionClass,
                               boolean multitenant) {
        super(multitenant);
        this.entityStorage = entityStorage;
        this.propertyStorage = propertyStorage;
        this.lastTimestampId = of(LAST_EVENT_TIMESTAMP_ID + projectionClass.getCanonicalName());
    }

    @Override
    public void writeLastHandledEventTime(Timestamp timestamp) {
        propertyStorage.write(lastTimestampId, timestamp);
    }

    @Nullable
    @Override
    public Timestamp readLastHandledEventTime() {
        final Optional<Timestamp> readTimestamp = propertyStorage.read(lastTimestampId);

        if ((!readTimestamp.isPresent()) || Validate.isDefault(readTimestamp.get())) {
            return null;
        }
        final Timestamp result = readTimestamp.get();
        return result;
    }

    @Override
    public RecordStorage<I> getRecordStorage() {
        return entityStorage;
    }

    @SuppressWarnings("unused")     // part of API
    protected DsPropertyStorage getPropertyStorage() {
        return propertyStorage;
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<I> ids) {
        return getRecordStorage().readMultiple(ids);
    }

    @Override
    protected Iterable<EntityStorageRecord> readMultipleRecords(Iterable<I> ids, FieldMask fieldMask) {
        return getRecordStorage().readMultiple(ids, fieldMask);
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllRecords() {
        return getRecordStorage().readAll();
    }

    @Override
    protected Map<I, EntityStorageRecord> readAllRecords(FieldMask fieldMask) {
        return getRecordStorage().readAll(fieldMask);
    }
}
