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

import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.Messages;
import org.spine3.protobuf.TypeUrl;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;

/**
 * Utility class for converting {@link Message proto messages} into {@link Entity Entities} and vise versa.
 *
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
/*package*/ class Entities {

    private static final String VALUE_PROPERTY_NAME = "value";

    private Entities() {
    }

    /**
     * Retrieves a message of given type, assignable from {@code Message}, from an {@link Entity}.
     *
     * <p>If passed {@link Entity} is {@code null}, a default instance for the given type is returned.
     *
     * @param entity source {@link Entity} to get message form
     * @param type   {@link TypeUrl} of required message
     * @param <M>    required message type
     * @return message contained in the {@link Entity}
     */
    @SuppressWarnings("unchecked")
    /*package*/ static <M extends Message> M entityToMessage(@Nullable Entity entity, TypeUrl type) {
        if (entity == null) {
            return defaultMessage(type);
        }

        final Blob value = entity.getBlob(VALUE_PROPERTY_NAME);
        final ByteString valueBytes = ByteString.copyFrom(value.toByteArray());

        final Any wrapped = Any.newBuilder()
                               .setValue(valueBytes)
                               .setTypeUrl(type.value())
                               .build();
        final M result = AnyPacker.unpack(wrapped);
        return result;
    }

    /**
     * Retrieves a {@link List} of messages of given type, assignable from {@code Message},
     * from a collection of {@link Entity Entities}.
     *
     * <p>If passed {@link Entity} is {@code null}, a default instance for given type is returned.
     *
     * @param entities a collection of the source {@link Entity Entities} to get message form
     * @param type     {@link TypeUrl} of required message
     * @param <M>      required message type
     * @return message contained in the {@link Entity}
     */
    @SuppressWarnings("unchecked")
    /*package*/ static <M extends Message> List<M> entitiesToMessages(Collection<Entity> entities, TypeUrl type) {
        if (entities.isEmpty()) {
            return Collections.emptyList();
        }

        final String typeName = type.value();
        final ImmutableList.Builder<M> messages = new ImmutableList.Builder<>();
        for (Entity entity : entities) {
            if (entity == null) {
                final M defaultMessage = defaultMessage(type);
                messages.add(defaultMessage);
                continue;
            }

            final Blob value = entity.getBlob(VALUE_PROPERTY_NAME);
            final ByteString valueBytes = ByteString.copyFrom(value.toByteArray());

            final Any wrapped = Any.newBuilder()
                                   .setValue(valueBytes)
                                   .setTypeUrl(typeName)
                                   .build();
            final M message = AnyPacker.unpack(wrapped);
            messages.add(message);
        }

        return messages.build();
    }

    /**
     * Generates an {@link Entity} with given {@link Key} and from given proto {@code Message}
     *
     * @param message source of data to be put into the {@link Entity}
     * @param key     instance of {@link Key} to be assigned to the {@link Entity}
     * @return new instance of {@link Entity} containing serialized proto message
     */
    @SuppressWarnings("ConstantConditions")
    /*package*/ static Entity messageToEntity(Message message, Key key) {
        checkArgument(message != null, "Message must not be null");
        checkArgument(key != null, "Key must not be null");

        final Any wrapped = AnyPacker.pack(message);
        final byte[] messageBytes = wrapped.getValue().toByteArray();
        final Blob valueBlob = Blob.copyFrom(messageBytes);
        final Entity entity = Entity.newBuilder(key)
                                    .set(VALUE_PROPERTY_NAME, valueBlob)
                                    .build();
        return entity;
    }

    @SuppressWarnings("unchecked")
    /*package*/ static <M extends Message> M defaultMessage(TypeUrl type) {
        final Class<M> messageClass = Messages.toMessageClass(type);
        checkState(messageClass != null, String.format(
                "Not found class for type url \"%s\". Try to rebuild the project",
                type.getTypeName()));
        final M message;
        try {
            final Method factoryMethod = messageClass.getDeclaredMethod("getDefaultInstance");
            message = (M) factoryMethod.invoke(null);
            return message;
        } catch (@SuppressWarnings("OverlyBroadCatchBlock") ReflectiveOperationException | ClassCastException e) {
            throw propagate(e);
        }
    }
}
