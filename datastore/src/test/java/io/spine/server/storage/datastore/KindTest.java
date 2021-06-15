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

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.type.TypeName;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`Kind` should")
final class KindTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void testNulls() {
        new NullPointerTester()
                .setDefault(TypeUrl.class, TypeUrl.from(Any.getDescriptor()))
                .setDefault(Descriptors.Descriptor.class, Any.getDescriptor())
                .setDefault(Message.class, Any.getDefaultInstance())
                .setDefault(TypeName.class, TypeName.of(Any.class))
                .testStaticMethods(Kind.class, NullPointerTester.Visibility.PACKAGE);
    }

    @Test
    @DisplayName("support equality")
    void testEquals() {
        Kind anyFromDesc = Kind.of(Any.getDescriptor());
        Kind anyFromTypeUrl = Kind.of(TypeUrl.of(Any.class));
        Kind anyFromString = Kind.of("google.protobuf.Any");

        Kind fieldMaskFromInstance = Kind.of(FieldMask.getDefaultInstance());
        Kind fieldMaskFromTypeName = Kind.of(TypeName.of(FieldMask.class));
        Kind fieldMaskFromDesc = Kind.of(FieldMask.getDescriptor());

        new EqualsTester()
                .addEqualityGroup(anyFromDesc, anyFromString, anyFromTypeUrl)
                .addEqualityGroup(fieldMaskFromDesc, fieldMaskFromInstance, fieldMaskFromTypeName)
                .testEquals();
    }

    @Test
    @DisplayName("not accept forbidden prefix")
    void testCheckValidity() {
        String invalidKind = "__my.invalid.type";
        assertThrows(IllegalArgumentException.class, () -> Kind.of(invalidKind));
    }

    @Test
    @DisplayName("construct from string")
    void testFromString() {
        String type = "my.custom.type";
        Kind kind = Kind.of(type);
        assertEquals(type, kind.value());
    }

    @Test
    @DisplayName("construct from `TypeUrl`")
    void testFromTypeUrl() {
        Descriptors.Descriptor descriptor = Any.getDescriptor();
        TypeUrl type = TypeUrl.from(descriptor);
        Kind kind = Kind.of(type);
        assertEquals(descriptor.getFullName(), kind.value());
        assertEquals(type.toTypeName()
                         .value(), kind.value());
    }

    @Test
    @DisplayName("construct from `Descriptor`")
    void testFromDescriptor() {
        Descriptors.Descriptor descriptor = Any.getDescriptor();
        Kind kind = Kind.of(descriptor);
        assertEquals(descriptor.getFullName(), kind.value());
    }

    @Test
    @DisplayName("construct from `Message`")
    void testFromMessage() {
        Message message = Any.getDefaultInstance();
        Kind kind = Kind.of(message);
        assertEquals(message.getDescriptorForType()
                            .getFullName(), kind.value());
    }

    @Test
    @DisplayName("construct from `TypeName`")
    void testFromTypeName() {
        Descriptors.Descriptor descriptor = Any.getDescriptor();
        TypeName type = TypeName.from(descriptor);
        Kind kind = Kind.of(type);
        assertEquals(descriptor.getFullName(), kind.value());
        assertEquals(type.value(), kind.value());
    }
}
