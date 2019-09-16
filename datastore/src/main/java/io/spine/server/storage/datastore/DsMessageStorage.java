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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;
import io.spine.reflect.GenericTypeIndex;
import io.spine.server.storage.AbstractStorage;
import io.spine.server.storage.ReadRequest;
import io.spine.string.Stringifiers;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;
import static io.spine.util.Exceptions.newIllegalStateException;
import static io.spine.util.Exceptions.unsupported;
import static java.util.stream.Collectors.toList;

/**
 * An abstract base for storages operating plain {@link Message}s in Datastore.
 *
 * <p>To store {@code Message}s representing {@link io.spine.server.entity.Entity Entity} states
 * refer to {@link DsRecordStorage} and its descendants.
 *
 * @param <I>
 *         the type of the stored message identifiers
 * @param <M>
 *         the type of the stored message
 * @param <R>
 *         the type of the read request specific to this storage
 */
public abstract class DsMessageStorage<I, M extends Message, R extends ReadRequest<I>>
        extends AbstractStorage<I, M, R> {

    /**
     * The wrapper over the Google Datastore to use in operation.
     */
    private final DatastoreWrapper datastore;

    /**
     * The type URL of the stored {@code Message}s.
     */
    private final TypeUrl typeUrl;

    /**
     * The Datastore {@link Kind} of the records for the stored {@code Messages}.
     */
    private final Kind kind;

    protected DsMessageStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(multitenant);
        this.datastore = datastore;

        @SuppressWarnings("unchecked")      // Ensured by the class declaration.
                Class<M> result = (Class<M>) GenericParameter.MESSAGE.argumentIn(getClass());
        typeUrl = TypeUrl.of(result);
        kind = Kind.of(typeUrl);
    }

    /**
     * Obtains an identifier for the message.
     */
    abstract I idOf(M message);

    /**
     * Obtains the array of the message columns to use while writing messages to the storage.
     */
    abstract MessageColumn<M>[] columns();

    /**
     * Converts the given message to {@link Entity}.
     */
    final Entity toEntity(M message) {
        I id = idOf(message);
        Key key = key(id);
        Entity.Builder builder = Entities.builderFromMessage(message, key);

        for (MessageColumn<M> value : columns()) {
            value.fill(builder, message);
        }
        return builder.build();
    }

    /**
     * Writes the message to the storage.
     *
     * <p>In case the record with the same ID already exists, the record is updated. Otherwise,
     * a new record is created.
     */
    public void write(M message) {
        checkNotNull(message);

        Entity entity = toEntity(message);
        datastore.createOrUpdate(entity);
    }

    /**
     * Writes the message to the storage.
     *
     * <p>The identifier is {@linkplain #idOf(Message) obtained} directly from the {@code message},
     * so {@code id} parameter passed to this method is ignored.
     *
     * @see #write(Message)
     */
    @Override
    public final void write(I id, M message) {
        checkNotNull(id);
        checkNotNull(message);

        write(message);
    }

    /**
     * Behaves similarly to {@link #write(Message)}, but performs the operation
     * in scope of a Datastore transaction.
     *
     * <p>If there is no active transaction at the moment, the new transaction is started
     * and committed. In case there is already an active transaction, the write operation
     * is performed in its scope.
     *
     * @param message the message to write
     */
    @SuppressWarnings("OverlyBroadCatchBlock")  // handling all possible transaction-related issues.
    final void writeTransactionally(M message) {
        checkNotNull(message);

        boolean txRequired = !datastore.isTransactionActive();
        if(txRequired) {
            datastore.startTransaction();
        }
        try {
            write(message);
            if(txRequired) {
                datastore.commitTransaction();
            }
        } catch (Exception e) {
            if(txRequired && datastore.isTransactionActive()) {
                datastore.rollbackTransaction();
            }
            throw newIllegalStateException("Error committing the transaction.", e);
        }
    }

    /**
     * Writes all the passed messages to the storage in a bulk.
     *
     * <p>Messages may either end up as the updates of existing Datastore records — in case
     * there are records with the same identifiers — or as new records.
     */
    public void writeAll(Iterable<M> messages) {
        checkNotNull(messages);

        List<Entity> entities =
                stream(messages)
                        .map(this::toEntity)
                        .collect(toList());
        datastore.createOrUpdate(entities);
    }

    /**
     * Reads all the messages from the storage.
     *
     * <p>Allows to customize the read query with the parameters such as filters and ordering
     * by passing them via {@code queryBuilder}.
     *
     * @param queryBuilder
     *         the partially composed query builder
     * @param readBatchSize
     *         the batch size to use while reading
     * @return an iterator over the result set
     * @see DatastoreWrapper#readAll(StructuredQuery, int)
     */
    Iterator<M> readAll(EntityQuery.Builder queryBuilder, int readBatchSize) {
        StructuredQuery<Entity> query =
                queryBuilder.setKind(kind.getValue())
                            .build();
        Iterator<Entity> iterator = datastore.readAll(query, readBatchSize);
        Iterator<M> transformed =
                Iterators.transform(iterator, (e) -> Entities.toMessage(e, typeUrl));
        return transformed;
    }

    /**
     * Removes all the messages from the storage by their identifiers.
     *
     * <p>If for any passed message there is no record with the same identifier, this message
     * is ignored.
     */
    public void removeAll(Iterable<M> messages) {
        checkNotNull(messages);

        Key[] keys = stream(messages)
                .map(m -> key(idOf(m)))
                .toArray(Key[]::new);
        datastore.delete(keys);
    }

    @Override
    public Optional<M> read(R request) {
        checkNotNull(request);

        I id = request.recordId();
        Key key = key(id);
        @Nullable Entity entity = datastore.read(key);
        if (entity == null) {
            return Optional.empty();
        }
        M message = Entities.toMessage(entity, typeUrl);
        return Optional.of(message);
    }

    /**
     * Always throws an {@link UnsupportedOperationException}.
     */
    @Override
    public Iterator<I> index() {
        throw unsupported(
                "`DsMessageStorage` does not provide `index` capabilities " +
                        "due to the enormous number of records stored.");
    }

    /**
     * Obtains the Datastore {@code Key} value out of the message identifier.
     */
    private Key key(I id) {
        String keyValue = Stringifiers.toString(id);
        return datastore.keyFor(kind, RecordId.of(keyValue));
    }

    /**
     * Enumeration of generic type parameters of this abstract class.
     */
    private enum GenericParameter implements GenericTypeIndex<DsMessageStorage> {

        /**
         * The index of the declaration of the generic parameter type {@code <I>} in
         * the {@link DsMessageStorage} abstract class.
         */
        ID(0),

        /**
         * The index of the declaration of the generic parameter type {@code <M>}
         * in the {@link DsMessageStorage} abstract class.
         */
        MESSAGE(1);

        private final int index;

        GenericParameter(int index) {
            this.index = index;
        }

        @Override
        public int index() {
            return index;
        }
    }
}
