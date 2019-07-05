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

import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxReadRequest;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.delivery.Page;
import io.spine.server.delivery.ShardIndex;
import io.spine.string.Stringifiers;

import java.util.Iterator;

import static com.google.cloud.Timestamp.fromProto;
import static com.google.cloud.datastore.StructuredQuery.OrderBy.asc;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.datastore.DsInboxStorage.Column.shardIndex;
import static io.spine.server.storage.datastore.DsInboxStorage.Column.whenReceived;

/**
 * {@link InboxStorage} implementation based on Google Cloud Datastore.
 */
public class DsInboxStorage
        extends DsMessageStorage<InboxMessageId, InboxMessage, InboxReadRequest>
        implements InboxStorage {

    /**
     * Actual value of how many messages are read from the storage per request.
     *
     * <p>Defaults to {@link DatastoreWrapper#MAX_ENTITIES_PER_WRITE_REQUEST}.
     */
    private final int readBatchSize;

    protected DsInboxStorage(DatastoreWrapper datastore, boolean multitenant, int readBatchSize) {
        super(datastore, multitenant);
        this.readBatchSize = readBatchSize;
    }

    protected DsInboxStorage(DatastoreWrapper datastore, boolean multitenant) {
        this(datastore, multitenant, DatastoreWrapper.MAX_ENTITIES_PER_WRITE_REQUEST);
    }

    @Override
    InboxMessageId idOf(InboxMessage message) {
        return message.getId();
    }

    @Override
    MessageColumn<InboxMessage>[] columns() {
        return Column.values();
    }

    @Override
    public Page<InboxMessage> readAll(ShardIndex index) {
        checkNotNull(index);

        EntityQuery.Builder builder =
                Query.newEntityQueryBuilder()
                     .setFilter(eq(shardIndex.columnName(), index.getIndex()))
                     .setOrderBy(asc(whenReceived.columnName()));
        Iterator<InboxMessage> iterator = readAll(builder, readBatchSize);
        return new InboxPage(iterator, readBatchSize);
    }

    /**
     * The columns of the {@code InboxMessage} kind in Datastore.
     */
    enum Column implements MessageColumn<InboxMessage> {

        signalId("signal_id", (m) -> {
            return StringValue.of(m.getSignalId()
                                   .getValue());
        }),

        inboxId("inbox_id", (m) -> {
            return StringValue.of(Stringifiers.toString(m.getInboxId()));
        }),

        shardIndex("inbox_shard", (m) -> {
            return LongValue.of(m.getShardIndex()
                                 .getIndex());
        }),

        ofTotalShards("of_total_inbox_shards", (m) -> {
            return LongValue.of(m.getShardIndex()
                                 .getOfTotal());
        }),

        isEvent("is_event", (m) -> {
            return BooleanValue.of(m.hasEvent());
        }),

        isCommand("is_command", (m) -> {
            return BooleanValue.of(m.hasCommand());
        }),

        label("label", (m) -> {
            return StringValue.of(m.getLabel()
                                   .toString());
        }),

        status("status", (m) -> {
            return StringValue.of(m.getStatus()
                                   .toString());
        }),

        whenReceived("when_received", (m) -> {
            return TimestampValue.of(fromProto(m.getWhenReceived()));
        });

        /**
         * The column name.
         */
        private final String name;

        /**
         * Obtains the value of the column from the given message.
         */
        @SuppressWarnings("NonSerializableFieldInSerializableClass") // This enum isn't serialized.
        private final Getter<InboxMessage> getter;

        Column(String name, Getter<InboxMessage> getter) {
            this.name = name;
            this.getter = getter;
        }

        @Override
        public String columnName() {
            return name;
        }

        @Override
        public Getter<InboxMessage> getter() {
            return getter;
        }
    }
}
