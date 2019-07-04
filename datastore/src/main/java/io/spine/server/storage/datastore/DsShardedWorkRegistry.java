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

import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.common.collect.ImmutableList;
import io.spine.server.ContextSpec;
import io.spine.server.NodeId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;

import java.util.Iterator;
import java.util.Optional;

import static com.google.cloud.Timestamp.fromProto;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static io.spine.base.Time.currentTime;
import static io.spine.server.storage.datastore.DatastoreWrapper.MAX_ENTITIES_PER_WRITE_REQUEST;
import static io.spine.server.storage.datastore.DsShardedWorkRegistry.Column.shardIndex;

/**
 * A {@link ShardedWorkRegistry} based on the Google Datastore storage API.
 *
 * <p>It is recommended to use this implementation with Cloud Firestore in Datastore mode,
 * as it provides the strong consistency for queries.
 */
public class DsShardedWorkRegistry
        extends DsMessageStorage<ShardIndex, ShardSessionRecord, ShardSessionReadRequest>
        implements ShardedWorkRegistry {

    private static final ContextSpec spec = ContextSpec.singleTenant("__DELIVERY__");

    /**
     * Creates an instance of registry using the {@link DatastoreStorageFactory} passed.
     *
     * <p>The storage initialized by this registry is always single-tenant, since its
     * records represent the application-wide registry of server nodes, which aren't split
     * by tenant.
     */
    public DsShardedWorkRegistry(DatastoreStorageFactory factory) {
        super(factory.datastoreFor(spec), false);
    }

    @Override
    public Optional<ShardProcessingSession> pickUp(ShardIndex index, NodeId nodeId) {
        boolean pickedAlready = isPickedAlready(index);
        if (pickedAlready) {
            return Optional.empty();
        }
        ShardSessionRecord ssr = newRecord(index, nodeId);
        write(ssr);

        DsShardProcessingSession result = new DsShardProcessingSession(ssr, () -> clearNode(ssr));
        return Optional.of(result);
    }

    /**
     * Clears the {@linkplain ShardSessionRecord#getPickedBy() node} of the passed session and
     * updates the record in the storage.
     */
    private void clearNode(ShardSessionRecord record) {
        ShardSessionRecord updated = record.toBuilder()
                                           .clearPickedBy()
                                           .vBuild();
        write(updated);
    }

    /**
     * Tells if the session with the passed index is currently picked by any node.
     */
    private boolean isPickedAlready(ShardIndex index) {
        ImmutableList<ShardSessionRecord> records = readByIndex(index);
        long nodesWithShard = records
                .stream()
                .filter(ShardSessionRecord::hasPickedBy)
                .count();
        return nodesWithShard > 0;
    }

    @Override
    ShardIndex idOf(ShardSessionRecord message) {
        return message.getIndex();
    }

    @Override
    MessageColumn<ShardSessionRecord>[] columns() {
        return Column.values();
    }

    private static ShardSessionRecord newRecord(ShardIndex index, NodeId nodeId) {
        return ShardSessionRecord.newBuilder()
                                 .setIndex(index)
                                 .setPickedBy(nodeId)
                                 .setWhenLastPicked(currentTime())
                                 .vBuild();
    }

    private ImmutableList<ShardSessionRecord> readByIndex(ShardIndex index) {
        EntityQuery.Builder query =
                Query.newEntityQueryBuilder()
                     .setFilter(eq(shardIndex.columnName(), index.getIndex()));
        Iterator<ShardSessionRecord> iterator = readAll(query, MAX_ENTITIES_PER_WRITE_REQUEST);
        return ImmutableList.copyOf(iterator);
    }

    /**
     * The columns of the {@link ShardSessionRecord} message stored in Datastore.
     */
    enum Column implements MessageColumn<ShardSessionRecord> {

        shardIndex("work_shard", (m) -> {
            return LongValue.of(m.getIndex()
                                 .getIndex());
        }),

        ofTotalShards("of_total_work_shards", (m) -> {
            return LongValue.of(m.getIndex()
                                 .getOfTotal());
        }),

        nodeId("nodeId", (m) -> {
            return StringValue.of(m.getPickedBy()
                                   .getValue());
        }),

        whenLastPicked("when_last_picked", (m) -> {
            return TimestampValue.of(fromProto(m.getWhenLastPicked()));
        });

        /**
         * The column name.
         */
        private final String name;

        /**
         * Obtains the value of the column from the given message.
         */
        @SuppressWarnings("NonSerializableFieldInSerializableClass")  // This enum isn't serialized.
        private final Getter<ShardSessionRecord> getter;

        Column(String name,
               Getter<ShardSessionRecord> getter) {
            this.name = name;
            this.getter = getter;
        }

        @Override
        public String columnName() {
            return name;
        }

        @Override
        public Getter<ShardSessionRecord> getter() {
            return getter;
        }
    }
}
