/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.google.common.base.Charsets;
import com.google.common.base.Utf8;
import com.google.devtools.cloudtrace.v2.TruncatableString;

import java.nio.charset.Charset;

/**
 * Utilities for working with {@link TruncatableString}.
 */
final class Truncate {

    /**
     * Prevents the utility class instantiation.
     */
    private Truncate() {
    }

    /**
     * Truncates the given string to the specified amount of bytes in encoding.
     *
     * @param value
     *         the original string
     * @param length
     *         the required max length
     * @return truncated string
     */
    static TruncatableString stringTo(String value, int length) {
        int fullLength = Utf8.encodedLength(value);
        if (fullLength <= length) {
            return stringOf(value, 0);
        } else {
            Charset utf8 = Charsets.UTF_8;
            byte[] utf8Bytes = value.getBytes(utf8);
            String utf8String = new String(utf8Bytes, 0, length, utf8);
            return stringOf(utf8String, fullLength - length);
        }
    }

    private static TruncatableString stringOf(String value, int removedBytes) {
        return TruncatableString
                .newBuilder()
                .setValue(value)
                .setTruncatedByteCount(removedBytes)
                .build();
    }
}
