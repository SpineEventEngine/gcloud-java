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

import com.google.common.io.BaseEncoding;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.KnownTypes;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.reflect.Classes;
import org.spine3.type.ClassName;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class for performing transformations on entity IDs.
 *
 * <p>Provides functionality for transforming numeric and {@link Message protobuf} IDs into strings and vise versa.
 *
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
/*package*/ class IdTransformer {

    @SuppressWarnings("HardcodedLineSeparator")
    private static final String REFLECTIVE_ERROR_MESSAGE_PATTERN
            = "Reflective operation error trying to parse %s instance from string \"%s\"\n";
    private static final String WRONG_ID_TYPE_ERROR_MESSAGE
            = "Only String, numeric and Proto-message identifiers are allowed in GAE storage implementation.";
    private static final int STRING_BUILDER_INITIAL_CAPACITY = 128;
    private static final String TYPE_PREFIX = "TYPE:";
    private static final String SERIALIZED_MESSAGE_BYTES_DIVIDER = "-";
    private static final String SERIALIZED_MESSAGE_BYTES_POSTFIX = ":END";
    private static final String SERIALIZED_MESSAGE_DIVIDER = "::";
    private static final String SERIALIZED_DEFAULT_MESSAGE = "DEFAULT";
    private static final String WRONG_OR_BROKEN_MESSAGE_ID = "Passed proto ID %s is wrong or broken.";
    private static final String UNABLE_TO_DETECT_GENERIC_TYPE = "Unable to detect generic type of ID: ";
    private static final Logger LOG = Logger.getLogger(IdTransformer.class.getName());
    private static final int SERIALIZED_BYTES_RADIX = 16;

    private IdTransformer() {
    }

    /**
     * Transforms given wildcard ID into it's string representation.
     *
     * @param id accepted types are:
     *           <ul>
     *           <li> {@link Number numeric IDs}
     *           <li> {@code string IDs}
     *           <li> {@link Message protobuf IDs}
     *           </ul>
     *
     *           <p>Note: there are some limitations on what a string ID can be.
     *           See <a href="https://cloud.google.com/appengine/docs/python/datastore/entities>Datastore docs</a>
     *           for more info.
     *
     *           <p>Note: keeping in mind those limitations, one should also remember not to use too heavy protobuf
     *           messages as IDs. Remember that the serialized message should not have more then 100 bytes,
     *           otherwise this may lead to unexpected errors.
     * @return string representation of the given ID.
     */
    /*package*/
    static String idToString(Object id) {
        final String idString;
        if (id instanceof String) { // String ID
            idString = (String) id;
        } else if (isOfSupportedNumberType(id.getClass())) { // Numeric ID
            idString = id.toString();
        } else { // Proto-Message ID
            checkArgument(id instanceof Message, WRONG_ID_TYPE_ERROR_MESSAGE);

            @SuppressWarnings("TypeMayBeWeakened")
            final Message message = (Message) id;
            final StringBuilder idBuilder = new StringBuilder(STRING_BUILDER_INITIAL_CAPACITY);
            final TypeUrl typeUrl = KnownTypes.getTypeUrl(ClassName.of(id.getClass()
                                                                         .getCanonicalName()));
            final String prefixedTypeUrl = typeUrl.getPrefix() + '/' + typeUrl.getTypeName();
            idBuilder
                    .append(TYPE_PREFIX)
                    .append(prefixedTypeUrl)
                    .append(SERIALIZED_MESSAGE_DIVIDER);
            final byte[] serializedMessage = message.toByteArray();
            if (serializedMessage.length > 0) {
                for (int i = 0; i < serializedMessage.length - 1; i++) {
                    final byte singleByte = serializedMessage[i];
                    final String stringByte = BaseEncoding.base16()
                                                          .encode(new byte[]{singleByte});
                    idBuilder.append(stringByte)
                             .append(SERIALIZED_MESSAGE_BYTES_DIVIDER);
                }
                final byte lastByte = serializedMessage[serializedMessage.length - 1];
                idBuilder.append(BaseEncoding.base16()
                                             .encode(new byte[]{lastByte}));
            } else {
                idBuilder.append(SERIALIZED_DEFAULT_MESSAGE);
            }

            idBuilder.append(SERIALIZED_MESSAGE_BYTES_POSTFIX);

            idString = idBuilder.toString();
        }

        return idString;
    }

    /**
     * Transforms ID from a {@code String} representation back to a {@link Number} or a {@link Message protobuf message}.
     *
     * @param stringId          the {@code String} representation of the ID
     * @param parametrizedClass {@link Class parametrized type} of the {@link org.spine3.server.entity.Entity} to restore ID for
     * @return generic ID matching the given {@code String} representation
     */
    @SuppressWarnings("unchecked")
    /*package*/ static <I> I idFromString(String stringId, @Nullable Class parametrizedClass) {
        final Class<I> idClass = getIdClass(stringId, parametrizedClass);
        final I id;
        if (isOfSupportedNumberType(idClass)) { // Numeric ID
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

    @SuppressWarnings("ConstantConditions") // Nullable argument parametrizedClass
    private static <I> Class<I> getIdClass(String stringId, @Nullable Class parametrizedClass) {
        if (parametrizedClass == null) {
            return tryAllSupportedClasses(stringId);
        }

        try {
            @SuppressWarnings("unchecked")  // cast should be safe, since the convention.
            final Class<I> result = Classes.getGenericParameterType(parametrizedClass, 0);
            return result;
        } catch (ClassCastException e) {
            LOG.warning(UNABLE_TO_DETECT_GENERIC_TYPE + e.toString());
            return tryAllSupportedClasses(stringId);
        }
    }

    @SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
    private static <I> Class<I> tryAllSupportedClasses(String stringId) {
        if (stringId.startsWith(TYPE_PREFIX) && stringId.endsWith(SERIALIZED_MESSAGE_BYTES_POSTFIX)) {
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

        final int typeStartIndex = stringId.indexOf(TYPE_PREFIX) + TYPE_PREFIX.length();
        final int typeEndIndex = stringId.indexOf(SERIALIZED_MESSAGE_DIVIDER);
        final String typeName = stringId.substring(typeStartIndex, typeEndIndex);

        final int dataStartIndex = stringId.indexOf(SERIALIZED_MESSAGE_DIVIDER) + SERIALIZED_MESSAGE_DIVIDER.length();
        final int dataEndIndex = stringId.lastIndexOf(SERIALIZED_MESSAGE_BYTES_POSTFIX);
        final String bytesString = stringId.substring(dataStartIndex, dataEndIndex);

        if (bytesString.equals(SERIALIZED_DEFAULT_MESSAGE)) {
            final TypeUrl typeUrl = TypeUrl.of(typeName);
            final Message id = Entities.defaultMessage(typeUrl);
            return id;
        }

        final String[] separateStringBytes = bytesString.split(SERIALIZED_MESSAGE_BYTES_DIVIDER);
        final byte[] messageBytes = new byte[separateStringBytes.length];
        for (int i = 0; i < messageBytes.length; i++) {
            final byte oneByte = Byte.parseByte(separateStringBytes[i], SERIALIZED_BYTES_RADIX);
            messageBytes[i] = oneByte;
        }

        final ByteString byteString = ByteString.copyFrom(messageBytes);
        final Any wrappedId = Any.newBuilder()
                                 .setValue(byteString)
                                 .setTypeUrl(typeName)
                                 .build();
        final Message id = AnyPacker.unpack(wrappedId);
        return id;
    }

    private static boolean isOfSupportedNumberType(Class<?> clazz) {
        return clazz.isAssignableFrom(Integer.TYPE)
                || clazz.isAssignableFrom(Long.TYPE)
                || clazz.isAssignableFrom(Integer.class)
                || clazz.isAssignableFrom(Long.class);
    }
}
