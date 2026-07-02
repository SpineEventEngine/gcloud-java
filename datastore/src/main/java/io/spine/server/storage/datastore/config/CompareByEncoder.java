/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.datastore.config;

import com.google.common.base.Splitter;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.spine.option.CompareByOption;
import io.spine.option.OptionsProto;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Encodes a message marked with the {@code (compare_by)} option into an order-preserving
 * {@code String} key.
 *
 * <p>The Datastore compares stored column values by their native ordering, so a comparable
 * message column can support {@code >}/{@code <}/{@code >=}/{@code <=} queries only if it is
 * stored as a value whose ordering matches the message's own ordering. This encoder builds
 * such a value by reproducing, as an {@linkplain OrderedBytes order-preserving byte key}, the
 * comparison the Spine compiler generates for the {@code (compare_by)} option:
 *
 * <ul>
 *     <li>the option's fields are compared in their declared order
 *         ({@code comparing(f0).thenComparing(f1)...});
 *     <li>a nested message field marked with {@code (compare_by)} is compared recursively;
 *     <li>a {@code Timestamp} or {@code Duration} field is compared by {@code seconds} then
 *         {@code nanos} (matching the comparators registered in
 *         {@link io.spine.compare.ComparatorRegistry ComparatorRegistry});
 *     <li>a well-known wrapper field ({@code Int32Value}, {@code StringValue}, ...) is compared
 *         by its {@code value};
 *     <li>the {@code descending} flag reverses the whole ordering.
 * </ul>
 *
 * <p>The option is read from the message descriptor at runtime (it has no source retention),
 * matching the intent stated in {@code spine/options.proto}: "Runtime comparators may use the
 * reflection API to compare field values."
 */
final class CompareByEncoder {

    private static final String TIMESTAMP = "google.protobuf.Timestamp";
    private static final String DURATION = "google.protobuf.Duration";
    private static final String WRAPPER_VALUE = "value";
    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";

    private static final Splitter PATH_SPLITTER = Splitter.on('.');

    /** Prevents instantiation of this utility class. */
    private CompareByEncoder() {
    }

    /**
     * Tells whether instances of the given message type carry the {@code (compare_by)} option
     * and are therefore encodable by this class.
     */
    static boolean isComparable(Descriptor type) {
        return type.getOptions()
                   .hasExtension(OptionsProto.compareBy);
    }

    /**
     * Encodes the given {@code (compare_by)} message into an order-preserving hex string.
     *
     * @throws IllegalArgumentException
     *         if the message type does not carry the {@code (compare_by)} option
     */
    static String encode(Message message) {
        var type = message.getDescriptorForType();
        checkArgument(isComparable(type),
                      "The message type `%s` is not marked with the `(compare_by)` option.",
                      type.getFullName());
        var bytes = new OrderedBytes();
        encodeComparable(message, bytes);
        var option = compareByOption(type);
        if (option.getDescending()) {
            bytes.invertAll();
        }
        return bytes.toHexString();
    }

    /**
     * Appends the comparison fields of a {@code (compare_by)} message, in their declared order.
     */
    private static void encodeComparable(Message message, OrderedBytes bytes) {
        var option = compareByOption(message.getDescriptorForType());
        for (var path : option.getFieldList()) {
            encodeFieldPath(message, path, bytes);
        }
    }

    /**
     * Resolves a (possibly dotted) field path to its leaf field and appends the leaf value.
     */
    private static void encodeFieldPath(Message message, String path, OrderedBytes bytes) {
        List<String> names = PATH_SPLITTER.splitToList(path);
        var current = message;
        for (var i = 0; i < names.size() - 1; i++) {
            var field = fieldNamed(current.getDescriptorForType(), names.get(i));
            current = (Message) current.getField(field);
        }
        var leaf = fieldNamed(current.getDescriptorForType(), names.get(names.size() - 1));
        encodeValue(leaf, current.getField(leaf), bytes);
    }

    private static void encodeValue(FieldDescriptor field, Object value, OrderedBytes bytes) {
        switch (field.getJavaType()) {
            case INT:
                bytes.putSignedLong(((Integer) value).longValue());
                break;
            case LONG:
                bytes.putSignedLong((Long) value);
                break;
            case BOOLEAN:
                bytes.putBoolean((Boolean) value);
                break;
            case FLOAT:
                bytes.putDouble(((Float) value).doubleValue());
                break;
            case DOUBLE:
                bytes.putDouble((Double) value);
                break;
            case STRING:
                bytes.putString((String) value);
                break;
            case ENUM:
                // Java enums compare by their ordinal, which matches the generated comparator.
                bytes.putSignedLong(((EnumValueDescriptor) value).getIndex());
                break;
            case MESSAGE:
                encodeMessageValue((Message) value, bytes);
                break;
            case BYTE_STRING:
            default:
                throw unsupported(field);
        }
    }

    /**
     * Appends a message-typed comparison field: a nested {@code (compare_by)} message,
     * a {@code Timestamp}/{@code Duration}, or a well-known wrapper.
     */
    private static void encodeMessageValue(Message value, OrderedBytes bytes) {
        var type = value.getDescriptorForType();
        if (isComparable(type)) {
            encodeComparable(value, bytes);
            return;
        }
        var typeName = type.getFullName();
        if (TIMESTAMP.equals(typeName) || DURATION.equals(typeName)) {
            // `seconds` then `nanos`, matching the `Timestamp`/`Duration` comparators registered
            // in `ComparatorRegistry`. This assumes a normalized value (for a `Duration`, `nanos`
            // has the same sign as `seconds`; for a `Timestamp`, `nanos` is in `[0, 1e9)`), which
            // well-formed protos always are. Note this two-field key differs from the standalone
            // `Duration` column mapping (`DsColumnMapping.ofDuration()`, a single total-nanos
            // `long`); the two encodings live in separate columns and are never compared together.
            bytes.putSignedLong(longField(value, SECONDS));
            bytes.putSignedLong(longField(value, NANOS));
            return;
        }
        var wrapped = type.findFieldByName(WRAPPER_VALUE);
        if (wrapped != null && type.getFields().size() == 1) {
            encodeValue(wrapped, value.getField(wrapped), bytes);
            return;
        }
        throw new IllegalArgumentException(format(
                "The message type `%s` cannot be used in an ordering comparison. It must be "
                        + "marked with `(compare_by)`, be a `Timestamp`/`Duration`, or be a "
                        + "well-known wrapper type.", typeName));
    }

    private static long longField(Message message, String fieldName) {
        var field = fieldNamed(message.getDescriptorForType(), fieldName);
        return ((Number) message.getField(field)).longValue();
    }

    private static FieldDescriptor fieldNamed(Descriptor type, String name) {
        var field = type.findFieldByName(name);
        checkArgument(field != null,
                      "The type `%s` has no field named `%s` referenced by `(compare_by)`.",
                      type.getFullName(), name);
        return field;
    }

    private static CompareByOption compareByOption(Descriptor type) {
        return type.getOptions()
                   .getExtension(OptionsProto.compareBy);
    }

    private static IllegalArgumentException unsupported(FieldDescriptor field) {
        return new IllegalArgumentException(format(
                "The field `%s` of type `%s` is not supported in an ordering comparison.",
                field.getFullName(), field.getType()));
    }
}
