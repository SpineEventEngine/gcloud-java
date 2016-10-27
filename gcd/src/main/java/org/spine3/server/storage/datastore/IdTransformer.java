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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.server.reflect.Classes;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Dmytro Dashenkov
 */
/*package*/ class IdTransformer {

    @SuppressWarnings("HardcodedLineSeparator")
    private static final String REFLECTIVE_ERROR_MESSAGE_PATTERN
            = "Reflective operation error trying to parse %s instance from string \"%s\"\n";
    private static final String WRONG_ID_TYPE_ERROR_MESSAGE
            = "Only String, numeric and Proto-message identifiers are allowed in GAE storage implementation.";
    private static final int STRING_BUILDER_INITIAL_CAPACITY = 128;
    private static final String TYPE_PREFIX = "TYPE::";
    private static final String SERIALIZED_MESSAGE_BYTES_DIVIDER = "-";
    private static final String SERIALIZED_MESSAGE_BYTES_POSTFIX = "::END";
    private static final String SERIALIZED_MESSAGE_DIVIDER = "::::";
    private static final String WRONG_OR_BROKEN_MESSAGE_ID = "Passed proto ID %s is wrong or broken.";
    private static final Logger LOG = Logger.getLogger(IdTransformer.class.getName());

    /*package*/ static String idToString(Object id) {
        final String idString;
        if (id instanceof String) { // String ID
            idString = (String) id;
        } else if (isNumber(id.getClass())) { // Numeric ID
            idString = id.toString();
        } else { // Proto-Message ID
            checkArgument(id instanceof Message, WRONG_ID_TYPE_ERROR_MESSAGE);

            final Message message = (Message) id;
            final StringBuilder idBuilder = new StringBuilder(STRING_BUILDER_INITIAL_CAPACITY);
            idBuilder
                    .append(TYPE_PREFIX)
                    .append(message.getDescriptorForType().getFullName())
                    .append("");
            final byte[] serializedMessage = message.toByteArray();
            for (byte b : serializedMessage) {
                idBuilder.append(b).append(SERIALIZED_MESSAGE_BYTES_DIVIDER);
            }
            idBuilder.append(SERIALIZED_MESSAGE_BYTES_POSTFIX);

            idString = idBuilder.toString();
        }

        return idString;
    }

    /*package*/ @SuppressWarnings("unchecked")
    static <I> I idFromString(String stringId, Class parametrizedClass) {
        final Class<I> idClass = getIdClass(stringId, parametrizedClass);
        final I id;
        if (isNumber(idClass)) { // Numeric ID
            final String typeName = idClass.getSimpleName();
            try {
                final Method parser = idClass.getDeclaredMethod("parse" + typeName, String.class);
                id = (I) parser.invoke(null, stringId);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                // Never happen
                LOG.warning(String.format(REFLECTIVE_ERROR_MESSAGE_PATTERN, typeName, stringId));
                return null;
            }
        } else if (idClass.isAssignableFrom(String.class)) { // String ID
            id = (I) stringId;
        } else { // Proto Message ID
            id = (I) protoIdFromString(stringId);
        }

        return id;
    }

    private static <I> Class<I> getIdClass(String stringId, Class parametrizedClass) {
        try {
            return Classes.getGenericParameterType(parametrizedClass, 0);
        } catch (ClassCastException e) {
            return tryAllSupportedClasses(stringId);
        }
    }

    @SuppressWarnings("unchecked")
    private static <I> Class<I> tryAllSupportedClasses(String stringId) {
        if (stringId.startsWith(TYPE_PREFIX)) {
            return (Class<I>) Message.class;
        } else {
            try {
                Long.parseLong(stringId);
                return (Class<I>) Long.class;
            } catch (NumberFormatException ignored) {
                return (Class<I>) String.class;
            }
        }
    }

    private static Message protoIdFromString(String stringId) {
        checkArgument(stringId.startsWith(TYPE_PREFIX), String.format(WRONG_OR_BROKEN_MESSAGE_ID, stringId));
        checkArgument(stringId.endsWith(SERIALIZED_MESSAGE_BYTES_POSTFIX), String.format(WRONG_OR_BROKEN_MESSAGE_ID, stringId));

        final String bytesString = stringId.substring(
                stringId.indexOf(SERIALIZED_MESSAGE_DIVIDER),
                stringId.lastIndexOf(SERIALIZED_MESSAGE_BYTES_POSTFIX));
        final String[] separateStringBytes = bytesString.split(SERIALIZED_MESSAGE_BYTES_DIVIDER);
        final byte[] messageBytes = new byte[separateStringBytes.length];
        for (int i = 0; i < messageBytes.length; i++) {
            final byte oneByte = Byte.parseByte(separateStringBytes[i]);
            messageBytes[i] = oneByte;
        }

        final String typeName = stringId.substring(
                stringId.indexOf(TYPE_PREFIX),
                stringId.indexOf(SERIALIZED_MESSAGE_DIVIDER));

        final ByteString byteString = ByteString.copyFrom(messageBytes);
        final Any wrappedId = Any.newBuilder()
                .setValue(byteString)
                .setTypeUrl(typeName)
                .build();
        final Message id = AnyPacker.unpack(wrappedId);
        return id;
    }


    private static boolean isNumber(Class<?> clss) {
        return clss.isPrimitive()
                || clss.isAssignableFrom(Integer.class)
                || clss.isAssignableFrom(Long.class);
    }
}
