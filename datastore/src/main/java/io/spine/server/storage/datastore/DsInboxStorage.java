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
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxReadRequest;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.delivery.Page;
import io.spine.server.delivery.ShardIndex;
import io.spine.string.Stringifiers;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.cloud.Timestamp.fromProto;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Exceptions.unsupported;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * {@link InboxStorage} implementation based on Google Cloud Datastore.
 */
public class DsInboxStorage extends InboxStorage {

    /**
     * The default value of how many messages are read from the storage per request.
     */
    private static final int DEFAULT_READ_BATCH_SIZE = 500;

    /**
     * The {@link Kind} of the stored messages.
     */
    private static final Kind KIND = Kind.of(InboxMessage.getDescriptor());

    /**
     * The {@link TypeUrl} of the stored messages.
     */
    private static final TypeUrl TYPE_URL = TypeUrl.from(InboxMessage.getDescriptor());

    /**
     * The wrapper over the Google Datastore to use in operation.
     */
    private final DatastoreWrapper datastore;

    /**
     * Actual value of how many messages are read from the storage per request.
     *
     * <p>Defaults to {@link #DEFAULT_READ_BATCH_SIZE}.
     */
    private final int readBatchSize;

    protected DsInboxStorage(DatastoreWrapper datastore, boolean multitenant, int readBatchSize) {
        super(multitenant);
        this.datastore = datastore;
        this.readBatchSize = readBatchSize;
    }

    protected DsInboxStorage(DatastoreWrapper datastore, boolean multitenant) {
        this(datastore, multitenant, DEFAULT_READ_BATCH_SIZE);
    }

    @Override
    public void write(InboxMessage message) {
        checkNotNull(message);
        Entity entity = toEntity(message);
        datastore.createOrUpdate(entity);
    }

    private Entity toEntity(InboxMessage message) {
        Key key = keyOf(message);
        Entity.Builder builder = Entities.builderFromMessage(message, key);

        for (Column value : Column.values()) {
            value.fill(builder, message);
        }
        return builder.build();
    }

    private Key keyOf(InboxMessage message) {
        InboxMessageId id = message.getId();
        return keyOf(id);
    }

    private Key keyOf(InboxMessageId id) {
        return datastore.keyFor(KIND, RecordId.ofEntityId(id));
    }

    @Override
    public void writeAll(Iterable<InboxMessage> messages) {
        List<Entity> entities =
                stream(messages.spliterator(), true)
                        .map(this::toEntity)
                        .collect(toList());
        datastore.createOrUpdate(entities);
    }

    @Override
    public Page<InboxMessage> readAll(ShardIndex index) {

        StructuredQuery<Entity> query =
                Query.newEntityQueryBuilder()
                     .setKind(KIND.getValue())
                     .setOrderBy(OrderBy.asc(Column.whenReceived.name))
                     .build();
        Iterator<Entity> iterator = datastore.readAll(query, readBatchSize);
        return new DsPage(iterator, readBatchSize);
    }

    @Override
    public void removeAll(Iterable<InboxMessage> messages) {
        Key[] keys = stream(messages.spliterator(), true)
                .map(this::keyOf)
                .toArray(Key[]::new);
        datastore.delete(keys);
    }

    @Override
    public Iterator<InboxMessageId> index() {
        throw unsupported(
                "`DsInboxStorage` does not provide `index` capabilities " +
                        "due to the enormous number of records stored.");
    }

    @Override
    public Optional<InboxMessage> read(InboxReadRequest request) {
        InboxMessageId id = request.recordId();
        Key key = keyOf(id);
        @Nullable Entity entity = datastore.read(key);
        if(entity == null) {
            return Optional.empty();
        }
        InboxMessage message = Entities.toMessage(entity, TYPE_URL);
        return Optional.of(message);
    }

    @Override
    public void write(InboxMessageId id, InboxMessage record) {
        write(record);
    }

    /**
     * A functional interface for functions that obtain values from the {@link InboxMessage} fields
     * for the respective {@link Entity} columns.
     */
    @Immutable
    @FunctionalInterface
    private interface ColumnValueGetter extends Function<InboxMessage, Value<?>> {
    }

    /**
     * The columns of the {@code InboxMessage} kind in Datastore.
     */
    private enum Column {

        signalId("signal_id", (m) -> {
            return StringValue.of(m.getSignalId()
                                   .getValue());
        }),

        inboxId("inbox_id", (m) -> {
            return StringValue.of(Stringifiers.toString(m.getInboxId()));
        }),

        shardIndex("shard_index", (m) -> {
            return LongValue.of(m.getShardIndex()
                                 .getIndex());
        }),

        ofTotalShards("of_total_shards", (m) -> {
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
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final ColumnValueGetter getter;

        Column(String name,
               ColumnValueGetter getter) {
            this.name = name;
            this.getter = getter;
        }

        /**
         * Fills a property of the given {@code builder} with the column value obtained
         * from the given {@code message}.
         *
         * @param builder
         *         the builder to set the value to
         * @param message
         *         the message which field value is going to be set
         * @return the same instance of {@code builder} with the property filled
         */
        private Entity.Builder fill(Entity.Builder builder, InboxMessage message) {
            Value<?> value = getter.apply(message);
            builder.set(name, value);
            return builder;
        }
    }

    /**
     * Datastores-specific implementation of {@link Page}.
     *
     * <p>Reads the messages from the passed iterator and forms a page no bigger than requested.
     */
    private static final class DsPage implements Page<InboxMessage> {

        private final Iterator<Entity> iterator;
        private final int batchSize;
        private final ImmutableList<InboxMessage> contents;

        private DsPage(Iterator<Entity> iterator, int size) {
            this.iterator = iterator;
            this.batchSize = size;
            ImmutableList.Builder<InboxMessage> builder = transform(iterator, batchSize);
            contents = builder.build();
        }

        private static ImmutableList.Builder<InboxMessage> transform(Iterator<Entity> iterator,
                                                                     int size) {
            ImmutableList.Builder<InboxMessage> builder = ImmutableList.builder();
            int contentSize = 0;
            while(contentSize < size && iterator.hasNext()) {
                Entity entity = iterator.next();
                InboxMessage message = Entities.toMessage(entity, TYPE_URL);
                builder.add(message);
                contentSize++;
            }
            return builder;
        }

        @Override
        public ImmutableList<InboxMessage> contents() {
            return contents;
        }

        @Override
        public int size() {
            return contents.size();
        }

        @Override
        public Optional<Page<InboxMessage>> next() {
            if(!iterator.hasNext()) {
                return Optional.empty();
            }
            DsPage page = new DsPage(iterator, batchSize);
            return Optional.of(page);
        }
    }
}
