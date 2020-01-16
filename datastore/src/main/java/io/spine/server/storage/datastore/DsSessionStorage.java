/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;

import java.util.Iterator;
import java.util.Optional;

import static com.google.cloud.Timestamp.fromProto;
import static io.spine.server.storage.datastore.DatastoreWrapper.MAX_ENTITIES_PER_WRITE_REQUEST;

/**
 * A Datastore-based storage which contains {@code ShardSessionRecord}s.
 */
public final class DsSessionStorage
        extends DsMessageStorage<ShardIndex, ShardSessionRecord, ShardSessionReadRequest> {

    private static final boolean multitenant = false;

    DsSessionStorage(DatastoreStorageFactory factory) {
        super(factory.systemWrapperFor(DsSessionStorage.class, multitenant), multitenant);
    }

    @Override
    ShardIndex idOf(ShardSessionRecord message) {
        return message.getIndex();
    }

    @Override
    MessageColumn<ShardSessionRecord>[] columns() {
        return Column.values();
    }

    /**
     * Obtains all the session records present in the storage.
     */
    Iterator<ShardSessionRecord> readAll() {
        return readAll(Query.newEntityQueryBuilder(), MAX_ENTITIES_PER_WRITE_REQUEST);
    }

    /**
     * Obtains the session record for the shard with the given index.
     */
    Optional<ShardSessionRecord> read(ShardIndex index) {
        return read(new ShardSessionReadRequest(index));
    }

    /**
     * The columns of the {@link ShardSessionRecord} message stored in Datastore.
     */
    private enum Column implements MessageColumn<ShardSessionRecord> {

        shard((m) -> {
            return LongValue.of(m.getIndex()
                                 .getIndex());
        }),

        total_shards((m) -> {
            return LongValue.of(m.getIndex()
                                 .getOfTotal());
        }),

        node((m) -> {
            return StringValue.of(m.getPickedBy()
                                   .getValue());
        }),

        when_last_picked((m) -> {
            return TimestampValue.of(fromProto(m.getWhenLastPicked()));
        });

        /**
         * Obtains the value of the column from the given message.
         */
        private final Getter<ShardSessionRecord> getter;

        Column(Getter<ShardSessionRecord> getter) {
            this.getter = getter;
        }

        @Override
        public String columnName() {
            return name();
        }

        @Override
        public Getter<ShardSessionRecord> getter() {
            return getter;
        }
    }
}
