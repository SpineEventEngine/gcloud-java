/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

import com.google.common.testing.NullPointerTester;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.Test;
import org.spine3.type.TypeName;
import org.spine3.type.TypeUrl;

import static org.junit.Assert.assertEquals;

/**
 * @author Dmytro Dashenkov
 */
public class KindShould {

    @Test
    public void be_null_safe() {
        new NullPointerTester()
                .setDefault(TypeUrl.class, TypeUrl.from(Any.getDescriptor()))
                .setDefault(Descriptors.Descriptor.class, Any.getDescriptor())
                .setDefault(Message.class, Any.getDefaultInstance())
                .setDefault(TypeName.class, TypeName.of(Any.class))
                .testStaticMethods(Kind.class, NullPointerTester.Visibility.PACKAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_accept_forbidden_prefix() {
        final String invalidKind = "__my.invalid.type";
        Kind.of(invalidKind);
    }

    @Test
    public void construct_from_string() {
        final String type = "my.custom.type";
        final Kind kind = Kind.of(type);
        assertEquals(type, kind.getValue());
    }

    @Test
    public void construct_from_TypeUrl() {
        final Descriptors.Descriptor descriptor = Any.getDescriptor();
        final TypeUrl type = TypeUrl.from(descriptor);
        final Kind kind = Kind.of(type);
        assertEquals(descriptor.getFullName(), kind.getValue());
        assertEquals(type.getTypeName(), kind.getValue());
    }

    @Test
    public void construct_from_descriptor() {
        final Descriptors.Descriptor descriptor = Any.getDescriptor();
        final Kind kind = Kind.of(descriptor);
        assertEquals(descriptor.getFullName(), kind.getValue());
    }

    @Test
    public void construct_from_Message() {
        final Message message = Any.getDefaultInstance();
        final Kind kind = Kind.of(message);
        assertEquals(message.getDescriptorForType().getFullName(), kind.getValue());
    }

    @Test
    public void construct_from_TypeName() {
        final Descriptors.Descriptor descriptor = Any.getDescriptor();
        final TypeName type = TypeName.from(descriptor);
        final Kind kind = Kind.of(type);
        assertEquals(descriptor.getFullName(), kind.getValue());
        assertEquals(type.value(), kind.getValue());
    }
}
