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

package io.spine.server.trace.stackdriver;

import io.spine.core.SignalId;

import java.util.UUID;

import static java.lang.Long.toHexString;

/**
 * A {@link TraceApiString} of a certain length.
 */
abstract class ShortTraceApiString extends TraceApiString {

    private static final long serialVersionUID = 0L;

    @SuppressWarnings("FormatStringAnnotation")
    ShortTraceApiString(String value) {
        super(value);
    }

    /**
     * Parses the given ID as a UUID and prints it as a hexadecimal string up to a given length.
     *
     * @param id
     *         the ID
     * @param resultLength
     *         max length
     * @return hexadecimal string of a given length or shorter
     */
    static String hexOfLength(SignalId id, int resultLength) {
        long mostSignificantBits = UUID.fromString(id.value())
                                       .getMostSignificantBits();
        long leastSignificantBits = UUID.fromString(id.value())
                                        .getLeastSignificantBits();
        String result = toHexString(mostSignificantBits) + toHexString(leastSignificantBits);
        return shorten(result, resultLength);
    }

    /**
     * Shortens the given string to the given length.
     *
     * @param value
     *         the original string
     * @param length
     *         the max length
     * @return the first {@code length} symbols of the original string or the whole string if its
     *         short enough
     */
    static String shorten(String value, int length) {
        return value.length() > length
               ? value.substring(0, length)
               : value;
    }
}
