/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.BlobValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.spine.server.storage.EntityField;
import io.spine.type.TypeUrl;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Streams.stream;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.storage.datastore.DsProperties.isArchived;
import static io.spine.server.storage.datastore.DsProperties.isDeleted;

/**
 * Utility class for converting {@linkplain Message proto messages} into
 * {@linkplain Entity entities} and vise versa.
 */
final class Entities {

    private static final Predicate<Entity> NOT_ARCHIVED_OR_DELETED = input -> {
        if (input == null) {
            return false;
        }
        boolean isNotArchived = !isArchived(input);
        boolean isNotDeleted = !isDeleted(input);
        return isNotArchived && isNotDeleted;
    };

    private static final String DEFAULT_MESSAGE_FACTORY_METHOD_NAME = "getDefaultInstance";

    /** Prevent utility class instantiation. */
    private Entities() {
    }

    /**
     * Retrieves a message of given type, assignable from {@code Message}, from an {@link Entity}.
     *
     * <p>If passed {@link Entity} is {@code null}, a default instance for the given type
     * is returned.
     *
     * @param entity source {@link Entity} to get message form
     * @param type   {@link TypeUrl} of required message
     * @param <M>    required message type
     * @return message contained in the {@link Entity}
     */
    static <M extends Message> M entityToMessage(@Nullable Entity entity, TypeUrl type) {
        if (entity == null) {
            return defaultMessage(type);
        }
        Any wrapped = toAny(entity, type);
        @SuppressWarnings("unchecked") // It's the caller responsibility to provide correct type.
        M result = (M) unpack(wrapped);
        return result;
    }

    private static Any toAny(Entity entity, TypeUrl type) {
        String typeName = type.value();
        Blob value = entity.getBlob(EntityField.bytes.toString());
        ByteString valueBytes = ByteString.copyFrom(value.toByteArray());
        Any result = Any
                .newBuilder()
                .setValue(valueBytes)
                .setTypeUrl(typeName)
                .build();
        return result;
    }

    /**
     * Maps the elements of the given {@code Iterator} from {@linkplain Entity Entities} to enclosed
     * {@linkplain Message Messages}.
     *
     * <p>The runtime type of the messages is specified within the {@code type} parameter.
     *
     * @param entities a collection of the source {@link Entity Entities} to get message form
     * @param type     {@link TypeUrl} of required message
     * @param <M>      required message type
     * @return message contained in the {@link Entity}
     */
    static <M extends Message>
    Iterator<M> entitiesToMessages(Iterator<Entity> entities, TypeUrl type) {
        Function<Entity, M> transformer = entityToMessage(type);
        return stream(entities).map(transformer)
                               .iterator();
    }

    /**
     * Retrieves a {@link Function} transforming {@linkplain Entity Entities} into
     * {@linkplain Message Messages} of the given type.
     *
     * @param type the desired type of the messages
     * @param <M>  the compile-time type of the messages
     * @return a {@link Function} transforming {@linkplain Entity Entities} into
     *         {@linkplain Message Messages}
     */
    static <M extends Message> Function<Entity, M> entityToMessage(TypeUrl type) {
        return entity -> entityToMessage(entity, type);
    }

    /**
     * Generates an {@link Entity} with given {@link Key} and from given proto {@code Message}.
     *
     * @param message source of data to be put into the {@link Entity}
     * @param key     instance of {@link Key} to be assigned to the {@link Entity}
     * @return new instance of {@link Entity} containing serialized proto message
     */
    static Entity messageToEntity(Message message, Key key) {
        checkNotNull(message);
        checkNotNull(key);

        byte[] messageBytes = message.toByteArray();
        Blob valueBlob = Blob.copyFrom(messageBytes);
        BlobValue blobValue = BlobValue
                .newBuilder(valueBlob)
                .setExcludeFromIndexes(true)
                .build();
        Entity entity = Entity
                .newBuilder(key)
                .set(EntityField.bytes.toString(), blobValue)
                .build();
        return entity;
    }

    static Predicate<Entity> activeEntity() {
        return NOT_ARCHIVED_OR_DELETED;
    }

    @SuppressWarnings("unchecked")
    static <M extends Message> M defaultMessage(TypeUrl type) {
        Class<?> messageClass = type.getJavaClass();
        checkState(messageClass != null, String.format(
                "Not found class for type url \"%s\". Try to rebuild the project",
                type.getTypeName()));
        M message;
        try {
            Method factoryMethod =
                    messageClass.getDeclaredMethod(DEFAULT_MESSAGE_FACTORY_METHOD_NAME);
            message = (M) factoryMethod.invoke(null);
            return message;
        } catch (@SuppressWarnings("OverlyBroadCatchBlock") ReflectiveOperationException e) {
            throw new IllegalStateException("Couldn't invoke static method "
                    + DEFAULT_MESSAGE_FACTORY_METHOD_NAME + " of class "
                    + messageClass.getCanonicalName(), e);
        }
    }
}
