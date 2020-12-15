/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;
import io.spine.annotation.SPI;
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
 * An abstract base for a storage persisting plain {@link Message}s in Datastore.
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
 * @apiNote Most of the methods are designed to be {@code protected} in order to provide SPI
 *         users with more features, rather than repeating the same code {@code DsMessageStorage}
 *         already has.
 */
@SPI
@SuppressWarnings({"WeakerAccess", "ClassWithTooManyMethods"})  // The methods are for SPI users.
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
    protected abstract I idOf(M message);

    /**
     * Obtains the array of the message columns to use while writing messages to the storage.
     */
    protected abstract MessageColumn<M>[] columns();

    @Override
    public Optional<M> read(R request) {
        checkNotNull(request);

        I id = request.recordId();
        Key key = key(id);
        @Nullable Entity entity = datastore.read(key);
        if (entity == null) {
            return Optional.empty();
        }
        M message = toMessage(entity);
        return Optional.of(message);
    }

    /**
     * Reads the message according to the passed read request.
     *
     * @param request
     *         the request describing the criteria to search for the message
     * @return the message, or {@code null} if there is no such a message
     */
    public Optional<M> readTransactionally(R request) {
        checkNotNull(request);

        I id = request.recordId();
        Key key = key(id);
        try (TransactionWrapper tx = datastore.newTransaction()) {
            Optional<Entity> result = tx.read(key);
            tx.commit();
            return result.map(this::toMessage);
        }
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
    protected Iterator<M> readAll(EntityQuery.Builder queryBuilder, int readBatchSize) {
        StructuredQuery<Entity> query = queryBuilder.setKind(kind.value())
                                                    .build();
        Iterator<Entity> iterator = datastore.readAll(query, readBatchSize);
        Iterator<M> transformed = asStateIterator(iterator);
        return transformed;
    }

    /**
     * Starts a new transaction and reads all the messages from the storage in its scope.
     *
     * @param queryBuilder
     *         the partially composed query builder
     * @param limit
     *         a maximum number of message to return
     * @return an iterator over the result set
     * @see #readAll(EntityQuery.Builder, int) for a non-transactional read
     */
    protected Iterator<M> readAllTransactionally(EntityQuery.Builder queryBuilder, int limit) {
        return readAllTransactionally(queryBuilder.setLimit(limit));
    }

    /**
     * Reads the messages from the storage.
     *
     * <p>The caller is responsible for setting the query limits and interpreting the results.
     * This call does not trigger reading of the entire dataset page-by-page.
     *
     * <p>The read operation is not performed transactionally.
     *
     * @param queryBuilder
     *         the partially composed query builder
     * @see DatastoreWrapper#read(StructuredQuery)
     * @see #readAll(EntityQuery.Builder, int)
     * @see #readAllTransactionally(EntityQuery.Builder) for a transactional read
     */
    protected Iterator<M> read(EntityQuery.Builder queryBuilder) {
        StructuredQuery<Entity> query = queryBuilder.setKind(kind.value())
                                                    .build();
        DsQueryIterator<Entity> iterator = datastore.read(query);
        Iterator<M> result = asStateIterator(iterator);
        return result;
    }

    /**
     * Starts new transaction and reads the messages from the storage
     * in the scope of the started transaction.
     *
     * <p>The caller is responsible for setting the query limits and interpreting the results.
     * This call does not trigger reading of the entire dataset page-by-page.
     *
     * @param queryBuilder
     *         the partially composed query builder
     * @see DatastoreWrapper#read(StructuredQuery)
     * @see #read(EntityQuery.Builder) for a non-transactional read operation
     */
    @SuppressWarnings("OverlyBroadCatchBlock")
    // Handling all exceptions similarly.
    protected Iterator<M> readAllTransactionally(EntityQuery.Builder queryBuilder) {
        StructuredQuery<Entity> query = queryBuilder.setKind(kind.value())
                                                    .build();
        try (TransactionWrapper tx = newTransaction()) {
            DsQueryIterator<Entity> iterator = tx.read(query);
            Iterator<M> result = asStateIterator(iterator);
            return result;
        } catch (RuntimeException e) {
            throw newIllegalStateException(
                    e, "Bulk reading from the kind `%s` in a transaction failed.", kind
            );
        }
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
     * Starts a new transaction.
     *
     * <p>Designed for usage in the descendants requiring more than just a read or just a write
     * operation per transaction.
     */
    protected final TransactionWrapper newTransaction() {
        return datastore.newTransaction();
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
     * Behaves similarly to {@link #write(Message)}, but performs the operation
     * in scope of a Datastore transaction.
     *
     * @param message
     *         the message to write
     */
    @SuppressWarnings("OverlyBroadCatchBlock")  // We react to all the exceptions similarly.
    protected final void writeTransactionally(M message) {
        checkNotNull(message);

        try (TransactionWrapper tx = newTransaction()) {
            Entity entity = toEntity(message);
            tx.createOrUpdate(entity);
            tx.commit();
        } catch (RuntimeException e) {
            throw newIllegalStateException(e,
                                           "Error writing a `%s` in a transaction.",
                                           message.getClass()
                                                  .getName());
        }
    }

    /**
     * Writes all the passed messages to the storage in a bulk.
     *
     * <p>Messages may either end up as the updates of existing Datastore records — in case
     * there are records with the same identifiers — or as new records.
     */
    @SuppressWarnings("OverlyBroadCatchBlock") // We react to all the exceptions similarly.
    protected void writeAllTransactionally(Iterable<M> messages) {
        checkNotNull(messages);

        try (TransactionWrapper tx = newTransaction()) {
            List<Entity> entities =
                    stream(messages)
                            .map(this::toEntity)
                            .collect(toList());
            tx.createOrUpdate(entities);
            tx.commit();
        } catch (RuntimeException e) {
            throw newIllegalStateException(
                    e, "Bulk write to the kind `%s` in a transaction failed.", kind
            );
        }
    }

    /**
     * Removes all the messages from the storage by their identifiers.
     *
     * <p>If for any passed message there is no record with the same identifier, this message
     * is ignored.
     */
    public void removeAll(Iterable<M> messages) {
        checkNotNull(messages);
        Key[] keys = toKeys(messages);
        datastore.delete(keys);
    }

    /**
     * Starts a new transaction and removes all the messages from the storage by their identifiers
     * in its scope.
     *
     * <p>If for any passed message there is no record with the same identifier, this message
     * is ignored.
     *
     * @see #removeAll(Iterable) for a non-transactional version
     */
    public void removeAllTransactionally(Iterable<M> messages) {
        checkNotNull(messages);
        Key[] keys = toKeys(messages);
        try (TransactionWrapper tx = newTransaction()) {
            tx.delete(keys);
            tx.commit();
        } catch (RuntimeException e) {
            throw newIllegalStateException(
                    e, "Bulk deletion from the kind `%s` in a transaction failed.", kind
            );
        }
    }

    private Key[] toKeys(Iterable<M> messages) {
        return stream(messages)
                .map(m -> key(idOf(m)))
                .toArray(Key[]::new);
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
    protected Key key(I id) {
        String keyValue = Stringifiers.toString(id);
        return key(RecordId.of(keyValue));
    }

    /**
     * Creates a new Datastore {@code Key} value from the provided record identifier.
     */
    protected final Key key(RecordId recordId) {
        return datastore.keyFor(kind, recordId);
    }

    /**
     * Creates a Datastore {@code Key} with an ancestor.
     *
     * @param keyValue
     *         the raw key value
     * @param ancestor
     *         the path to ancestor
     * @return a new {@code Key} instance
     */
    protected final Key keyWithAncestor(String keyValue, PathElement ancestor) {
        Key result = datastore.keyFactory(kind)
                              .addAncestor(ancestor)
                              .newKey(keyValue);
        return result;
    }

    protected final Key parentKey(Kind parentKind, Object parentId) {
        String keyValue = Stringifiers.toString(parentId);
        return datastore.keyFactory(parentKind)
                        .newKey(keyValue);
    }

    /**
     * Converts the given message to {@link Entity}.
     */
    protected final Entity toEntity(M message) {
        I id = idOf(message);
        Key key = key(id);
        Entity.Builder builder = Entities.builderFromMessage(message, key);

        for (MessageColumn<M> value : columns()) {
            value.fill(builder, message);
        }
        return builder.build();
    }

    /**
     * Converts the given {@link Entity} to message.
     */
    protected final M toMessage(Entity e) {
        return Entities.toMessage(e, typeUrl);
    }

    private Iterator<M> asStateIterator(Iterator<Entity> iterator) {
        return Iterators.transform(iterator, this::toMessage);
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
