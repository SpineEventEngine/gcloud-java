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

import com.google.protobuf.Timestamp;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.ProjectionStorage;

import javax.annotation.Nullable;

/**
 * @author Mikhail Mikhaylov
 */
/* package */ class DsProjectionStorage<I> extends ProjectionStorage<I> {

    private static final String LAST_EVENT_TIMESTAMP_ID = "Datastore-event-timestamp-";

    private final DsEntityStorage<I> entityStorage;
    private final DsPropertyStorage propertyStorage;

    private final String lastTimestampId;

    /* package */
    static <I> DsProjectionStorage<I> newInstance(DsEntityStorage<I> entityStorage,
                                                  DsPropertyStorage propertyStorage,
                                                  Class<? extends Entity<I, ?>> projectionClass) {
        return new DsProjectionStorage<>(entityStorage, propertyStorage, projectionClass);
    }

    private DsProjectionStorage(DsEntityStorage<I> entityStorage,
                                DsPropertyStorage propertyStorage,
                                Class<? extends Entity<I, ?>> projectionClass) {
        this.entityStorage = entityStorage;
        this.propertyStorage = propertyStorage;

        // TODO:2016-04-21:mikhail.mikhaylov: We should use proto class name instead of java's one.
        lastTimestampId = LAST_EVENT_TIMESTAMP_ID + projectionClass.getCanonicalName();
    }

    @Override
    public void writeLastHandledEventTime(Timestamp timestamp) {
        propertyStorage.write(lastTimestampId, timestamp);
    }

    @Nullable
    @Override
    public Timestamp readLastHandledEventTime() {
        return propertyStorage.read(lastTimestampId);
    }

    @Override
    public EntityStorage<I> getEntityStorage() {
        return entityStorage;
    }
}
