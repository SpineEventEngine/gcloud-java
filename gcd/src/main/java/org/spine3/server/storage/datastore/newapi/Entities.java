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

package org.spine3.server.storage.datastore.newapi;

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

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;

/**
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
public class Entities {

    private static final String VALUE_PROPERTY_NAME = "value";

    private Entities() {
    }

    @SuppressWarnings("unchecked")
    public static <M extends Message> M entityToMessage(Entity entity, TypeUrl type) {
        if (entity == null) {
            return defaultMessage(type);
        }

        final Blob value = entity.getBlob(VALUE_PROPERTY_NAME);
        final ByteString valueBytes = ByteString.copyFrom(value.toByteArray());

        final Any wrapped = Any.newBuilder()
                .setValue(valueBytes)
                .setTypeUrl(type.value())
                .build();
        return AnyPacker.unpack(wrapped);
    }

    @SuppressWarnings("unchecked")
    public static <M extends Message> List<M> entitiesToMessages(Collection<Entity> entities, TypeUrl type) {
        if (entities == null || entities.isEmpty()) {
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

    public static Entity messageToEntity(Message message, Key key) {
        checkArgument(message != null, "Message must not be null");
        checkArgument(key != null, "Key must not be null");

        final Any wrapped = AnyPacker.pack(message);
        final byte[] messageBytes = wrapped.getValue().toByteArray();
        final Blob valueBlob = Blob.copyFrom(messageBytes);
        final Entity entity = Entity.builder(key)
                .set(VALUE_PROPERTY_NAME, valueBlob)
                .build();
        return entity;
    }

    @SuppressWarnings("unchecked")
    private static <M extends Message> M defaultMessage(TypeUrl type) {
        final Class<M> messageClass = Messages.toMessageClass(type);
        checkState(messageClass != null, String.format(
                "Not found class for type url \"%s\". Try to rebuild project",
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
