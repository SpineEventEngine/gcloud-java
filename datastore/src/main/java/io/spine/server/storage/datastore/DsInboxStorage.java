/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.TimestampValue;
import com.google.common.collect.Iterables;
import com.google.protobuf.Timestamp;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxMessageStatus;
import io.spine.server.delivery.InboxReadRequest;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.delivery.Page;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.string.Stringifiers;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.cloud.Timestamp.fromProto;
import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.OrderBy.asc;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.hasAncestor;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.delivery.InboxMessageStatus.TO_DELIVER;
import static java.util.Arrays.copyOfRange;

/**
 * {@link InboxStorage} implementation based on Google Cloud Datastore.
 *
 * <p>Allows to run on top of either Datastore in native mode or Firestore in Datastore mode.
 *
 * <p>When operating in a native Datastore mode, no messages are modified or read in transactions
 * and no parent-child relations are used to store the records. In practice, such an approach
 * means there is no guarantee for the read, write and delete operations to be executed in
 * the order of calling.
 *
 * <p>For Firestore in Datastore mode there are few improvements:
 *
 * <ul>
 *     <li>The {@code InboxMessage} records are stored as children of a common parent, grouped by
 *     the {@code ShardIndex}.
 *
 *     <li>All read, write and remove operations are performed transactionally, i.e. taking
 *     the ancestor filter and composite Datastore keys into account. As Datastore guarantees
 *     the serializable isolation for transactions, all the modifications of the records
 *     are executed in a strict order.
 * </ul>
 *
 * <p>The unit tests are launched on the local Datastore emulator which only supports Datastore
 * native mode at the moment. Therefore, by default, {@code DsInboxStorage}
 * {@linkplain DsInboxStorage#DsInboxStorage(DatastoreWrapper, boolean) is created} in this mode
 *  as well.
 */
public class DsInboxStorage
        extends DsMessageStorage<InboxMessageId, InboxMessage, InboxReadRequest>
        implements InboxStorage {

    private final Client client;

    private static final Kind PARENT_KIND = Kind.of(
            TypeUrl.of(ShardSessionRecord.getDefaultInstance())
    );

    /**
     * Creates the instance of {@code DsInboxStorage} operating with Datastore database
     * in {@link DatastoreMode#NATIVE native} mode.
     *
     * @param datastore
     *         a datastore wrapper
     * @param multitenant
     *         whether this storage should support multi-tenancy
     */
    protected DsInboxStorage(DatastoreWrapper datastore, boolean multitenant) {
        this(datastore, multitenant, DatastoreMode.NATIVE);
    }

    /**
     * Creates the instance of {@code DsInboxStorage} which takes the mode in which Datastore
     * database was created.
     *
     * <p>The operations performed with the records are adjusted according to the limits implied
     * by the Datastore operation mode.
     *
     * @param datastore
     *         a datastore wrapper
     * @param multitenant
     *         whether this storage should support multi-tenancy
     * @param mode
     *         a mode in which the underlying Datastore database operates
     */
    protected DsInboxStorage(DatastoreWrapper datastore, boolean multitenant, DatastoreMode mode) {
        super(datastore, multitenant);
        this.client = mode == DatastoreMode.NATIVE
                        ? new NativeClient()
                        : new FirestoreClient();
    }

    @Override
    protected InboxMessageId idOf(InboxMessage message) {
        return message.getId();
    }

    @Override
    protected final Key key(InboxMessageId id) {
        return client.key(id);
    }

    @Override
    protected MessageColumn<InboxMessage>[] columns() {
        return Column.values();
    }

    @Override
    public Page<InboxMessage> readAll(ShardIndex index, int pageSize) {
        checkNotNull(index);
        InboxPage page = new InboxPage(
                sinceWhen -> {
                    EntityQuery.Builder query = queryPage(index, sinceWhen);
                    return client.readAll(query, pageSize);
                });
        return page;
    }

    @Override
    public Optional<InboxMessage> newestMessageToDeliver(ShardIndex index) {
        EntityQuery.Builder builder = queryInShard(index, statusFilter(TO_DELIVER)).setLimit(1);
        Iterator<InboxMessage> iterator = client.readAll(builder);
        if (iterator.hasNext()) {
            return Optional.of(iterator.next());
        }
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Writes the message in a new transaction.
     */
    @Override
    public void write(InboxMessage message) {
        client.write(message);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Writes all the messages in a single new transaction.
     */
    @Override
    public void writeAll(Iterable<InboxMessage> messages) {
        client.writeAll(messages);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Removes all the messages in a single new transaction.
     */
    @Override
    public void removeAll(Iterable<InboxMessage> messages) {
        client.removeAll(messages);
    }

    private EntityQuery.Builder queryPage(ShardIndex index, @Nullable Timestamp sinceWhen) {
        return queryByTime(index, sinceWhen)
                .setOrderBy(asc(Column.receivedAt.columnName()),
                            asc(Column.version.columnName()));
    }

    private EntityQuery.Builder queryByTime(ShardIndex index, @Nullable Timestamp sinceWhen) {
        return sinceWhen == null
               ? queryInShard(index)
               : queryInShard(index, timeFilter(sinceWhen));
    }

    private EntityQuery.Builder queryInShard(ShardIndex index,
                                             StructuredQuery.Filter... additionalFilters) {
        return client.queryInShard(index, additionalFilters);
    }

    private static PropertyFilter statusFilter(InboxMessageStatus status) {
        return eq(Column.status.columnName(), status.toString());
    }

    private static PropertyFilter timeFilter(Timestamp sinceWhen) {
        return gt(Column.receivedAt.columnName(),
                  fromProto(sinceWhen));
    }

    private static StructuredQuery.Filter[] columnFilters(ShardIndex index,
                                                          StructuredQuery.Filter... moreFilters) {
        int indexValue = index.getIndex();
        int totalValue = index.getOfTotal();
        List<StructuredQuery.Filter> filters = new ArrayList<>(Arrays.asList(moreFilters));
        filters.add(eq(Column.shardIndex.columnName(), indexValue));
        filters.add(eq(Column.ofTotalShards.columnName(), totalValue));

        return Iterables.toArray(filters,
                                 StructuredQuery.Filter.class);
    }

    /**
     * The columns of the {@code InboxMessage} kind in Datastore.
     */
    private enum Column implements MessageColumn<InboxMessage> {

        signalId("signal_id", (m) -> {
            return StringValue.of(m.getSignalId()
                                   .getValue());
        }),

        inboxId("inbox_id", (m) -> {
            return StringValue.of(Stringifiers.toString(m.getInboxId()));
        }),

        shardIndex("inbox_shard", (m) -> {
            return LongValue.of(m.shardIndex()
                                 .getIndex());
        }),

        ofTotalShards("of_total_inbox_shards", (m) -> {
            return LongValue.of(m.shardIndex()
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

        receivedAt("received_at", (m) -> {
            return TimestampValue.of(fromProto(m.getWhenReceived()));
        }),

        version("version", (m) -> {
            return LongValue.of(m.getVersion());
        });

        /**
         * The column name.
         */
        private final String name;

        /**
         * Obtains the value of the column from the given message.
         */
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

    /**
     * A client to the Datastore instance which implementation depends on the mode
     * in which Datastore runs.
     */
    private interface Client {

        /**
         * Creates a new key for the given identifier.
         */
        Key key(InboxMessageId id);

        /**
         * Reads all messages according to the passed query.
         */
        Iterator<InboxMessage> readAll(EntityQuery.Builder query);

        /**
         * Reads all messages according to the passed query and the maximum size of the elements
         * returned in a single result set.
         */
        Iterator<InboxMessage> readAll(EntityQuery.Builder query, int size);

        /**
         * Writes the message to the storage.
         *
         * <p>The message is updated by its key if it exists, or created if not.
         */
        void write(InboxMessage message);

        /**
         * Writes all the passed messages to the storage in a single operation.
         *
         * <p>Any messages which keys are not present in the underlying storage will be created
         * as new.
         */
        void writeAll(Iterable<InboxMessage> messages);

        /**
         * Removes all the messages in a single operation.
         *
         * <p>Any messages absent in the underlying storage are ignored.
         */
        void removeAll(Iterable<InboxMessage> messages);

        /**
         * Creates a query filtering the {@code InboxMessage}s according to the shard index
         * and the additional filters passed.
         */
        EntityQuery.Builder queryInShard(ShardIndex index,
                                         StructuredQuery.Filter... additionalFilters);
    }

    /**
     * An implementation of the client specific to the Firestore in Datastore mode.
     *
     * @see DsInboxStorage class-level docs for more details
     */
    private final class FirestoreClient implements Client {

        @Override
        public Key key(InboxMessageId id) {
            String keyValue = id.getUuid();
            String parentKeyValue = Stringifiers.toString(id.getIndex());
            PathElement ancestor = PathElement.of(PARENT_KIND.value(), parentKeyValue);
            return keyWithAncestor(keyValue, ancestor);
        }

        @Override
        public Iterator<InboxMessage> readAll(EntityQuery.Builder query) {
            return readAllTransactionally(query);
        }

        @Override
        public Iterator<InboxMessage> readAll(EntityQuery.Builder query, int size) {
            return readAllTransactionally(query, size);
        }

        @Override
        public void write(InboxMessage message) {
            writeTransactionally(message);
        }

        @Override
        public void writeAll(Iterable<InboxMessage> messages) {
            writeAllTransactionally(messages);
        }

        @Override
        public void removeAll(Iterable<InboxMessage> messages) {
            removeAllTransactionally(messages);
        }

        @Override
        public EntityQuery.Builder queryInShard(ShardIndex index,
                                                StructuredQuery.Filter... additionalFilters) {
            Key parentKey = parentKey(PARENT_KIND, index);
            StructuredQuery.Filter[] columnFilters = columnFilters(index, additionalFilters);

            PropertyFilter ancestorFilter = hasAncestor(parentKey);
            return Query.newEntityQueryBuilder()
                        .setFilter(and(ancestorFilter, columnFilters));
        }
    }

    /**
     * An implementation of the client specific to the Datastore in native mode.
     *
     * @see DsInboxStorage class-level docs for more details
     */
    private final class NativeClient implements Client {

        @Override
        public Key key(InboxMessageId id) {
            String rawValue = id.getUuid();
            return DsInboxStorage.super.key(RecordId.of(rawValue));
        }

        @Override
        public Iterator<InboxMessage> readAll(EntityQuery.Builder query) {
            return DsInboxStorage.super.read(query);
        }

        @Override
        public Iterator<InboxMessage> readAll(EntityQuery.Builder query, int size) {
            EntityQuery.Builder withLimit = query.setLimit(size);
            return DsInboxStorage.super.read(withLimit);
        }

        @Override
        public void write(InboxMessage message) {
            DsInboxStorage.super.write(message);
        }

        @Override
        public void writeAll(Iterable<InboxMessage> messages) {
            DsInboxStorage.super.writeAll(messages);
        }

        @Override
        public void removeAll(Iterable<InboxMessage> messages) {
            DsInboxStorage.super.removeAll(messages);
        }

        @Override
        public EntityQuery.Builder queryInShard(ShardIndex index,
                                                StructuredQuery.Filter... additionalFilters) {
            StructuredQuery.Filter[] columnFilters = columnFilters(index, additionalFilters);
            return Query.newEntityQueryBuilder()
                        .setFilter(and(columnFilters[0],
                                       copyOfRange(columnFilters, 1, columnFilters.length)));
        }
    }
}
