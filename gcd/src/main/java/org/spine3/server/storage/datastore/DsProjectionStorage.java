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
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.ProjectionStorage;

import javax.annotation.Nullable;

/**
 * @author Mikhail Mikhaylov
 */
/* package */ class DsProjectionStorage<I> extends ProjectionStorage<I> {

    // TODO:2016-03-31:mikhail.mikhaylov: Find a way to reserve this id or store timestamps in some other way.
    private static final String LAST_EVENT_TIMESTAMP_ID = "Datastore-event-timestamp";

    private final DsEntityStorage<I> entityStorage;
    private final DsPropertyStorage propertyStorage;

    /* package */
    static <I> DsProjectionStorage<I> newInstance(DsEntityStorage<I> entityStorage,
                                                  DsPropertyStorage propertyStorage) {
        return new DsProjectionStorage<>(entityStorage, propertyStorage);
    }

    private DsProjectionStorage(DsEntityStorage<I> entityStorage, DsPropertyStorage propertyStorage) {
        this.entityStorage = entityStorage;
        this.propertyStorage = propertyStorage;
    }

    @Override
    public void writeLastHandledEventTime(Timestamp timestamp) {
        propertyStorage.write(LAST_EVENT_TIMESTAMP_ID, timestamp);
    }

    @Nullable
    @Override
    public Timestamp readLastHandledEventTime() {
        return propertyStorage.read(LAST_EVENT_TIMESTAMP_ID);
    }

    @Override
    public EntityStorage<I> getEntityStorage() {
        return entityStorage;
    }
}
