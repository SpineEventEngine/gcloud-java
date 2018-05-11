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

package io.spine.web;

import javax.servlet.http.HttpServlet;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static io.spine.util.Exceptions.unsupported;

/**
 * An {@link HttpServlet} which cannot be serialized.
 *
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("serial")
abstract class NonSerializableServlet extends HttpServlet {

    // Disabled serialization.
    // ---------------------

    /**
     * Blocks the Java serialization mechanism.
     *
     * <p>It is a common case for a servlet not to be able to serialize its fields.
     * Thus, any servlet class derived from {@code NonSerializableServlet} refuses
     * the serialization bequest.
     *
     * @throws UnsupportedOperationException always
     */
    private void writeObject(ObjectOutputStream ignored) throws UnsupportedOperationException {
        throw unsupported("%s serialization is not supported.", getClass().getSimpleName());
    }

    /**
     * Blocks the Java serialization mechanism.
     *
     * @throws UnsupportedOperationException always
     * @see #writeObject(ObjectOutputStream)
     */
    private void readObject(ObjectInputStream ignored) throws UnsupportedOperationException {
        throw unsupported("%s deserialization is not supported.", getClass().getSimpleName());
    }
}
