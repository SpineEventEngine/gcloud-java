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

import java.io.ByteArrayOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An accumulator of an <i>order-preserving</i> byte key.
 *
 * <p>Values appended to an instance are encoded so that the lexicographical (unsigned,
 * byte-by-byte) ordering of two resulting keys matches the semantic ordering of the
 * appended values. Concatenating several values keeps the "compare by the first field,
 * then by the second, and so on" semantics, exactly as
 * {@link java.util.Comparator#thenComparing(java.util.Comparator) Comparator.thenComparing()}.
 *
 * <p>The final key is rendered as a lowercase hex {@linkplain #toHexString() string}. Hex
 * is used so the key is always a valid, comparison-stable {@code String}: the ASCII digits
 * {@code 0}–{@code 9} and letters {@code a}–{@code f} sort in the same order as the raw
 * bytes they represent, and a two-characters-per-byte fixed width keeps that ordering exact.
 * A {@code String} (rather than a {@code Blob}) is required because the in-memory read paths
 * only extract {@code STRING}/{@code LONG}/{@code DOUBLE}/{@code BOOLEAN}/{@code TIMESTAMP}
 * Datastore values for comparison and sorting.
 *
 * <p>Encoding rules (all multibyte integers are big-endian):
 * <ul>
 *     <li>signed integrals — the sign bit is flipped, so the unsigned byte order matches
 *         the signed numeric order;
 *     <li>{@code double}/{@code float} — the IEEE-754 bits are transformed so that the
 *         byte order matches the numeric order (negatives fully inverted, non-negatives
 *         get the sign bit flipped);
 *     <li>{@code boolean} — a single {@code 0x00}/{@code 0x01} byte;
 *     <li>{@code String} — UTF-8 bytes with every {@code 0x00} escaped as {@code 0x00 0xFF}
 *         and a {@code 0x00 0x01} terminator. The terminator makes the encoding
 *         self-delimiting and preserves the "a" &lt; "ab" prefix ordering.
 * </ul>
 *
 * <p>This class is not thread-safe; create one instance per key being built.
 */
final class OrderedBytes {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /** Escape byte introducing a special two-byte sequence within a string segment. */
    private static final int STRING_ESCAPE = 0x00;

    /** Second byte of an escaped {@code 0x00} within a string ({@code 0x00 0xFF}). */
    private static final int STRING_ESCAPED_NUL = 0xFF;

    /** Second byte of a string terminator ({@code 0x00 0x01}). */
    private static final int STRING_TERMINATOR = 0x01;

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    /**
     * Appends a signed integral value using eight big-endian bytes with the sign bit flipped.
     *
     * <p>Narrower signed types ({@code int32}, {@code sint32}, enum indices) are widened to
     * {@code long} first, which preserves their order.
     */
    void putSignedLong(long value) {
        putRawLong(value ^ Long.MIN_VALUE);
    }

    /**
     * Appends a {@code double} (or a widened {@code float}) so that the byte order matches the
     * numeric order.
     */
    void putDouble(double value) {
        var bits = Double.doubleToLongBits(value);
        // If the sign bit is set (negative), flip all bits; otherwise flip just the sign bit.
        bits ^= (bits >> 63) | Long.MIN_VALUE;
        putRawLong(bits);
    }

    /** Appends a single byte for the given boolean ({@code false} sorts before {@code true}). */
    void putBoolean(boolean value) {
        buffer.write(value ? 1 : 0);
    }

    /**
     * Appends the UTF-8 bytes of the given string in a self-delimiting, order-preserving form.
     *
     * <p>UTF-8 byte order equals Unicode code point order, which matches
     * {@link String#compareTo(String) String.compareTo()} for all Basic Multilingual Plane text.
     * The two orderings can differ only for strings containing supplementary (non-BMP) characters,
     * which {@code String.compareTo()} orders by UTF-16 code unit.
     */
    void putString(String value) {
        var utf8 = value.getBytes(UTF_8);
        for (var b : utf8) {
            var unsigned = b & 0xFF;
            buffer.write(unsigned);
            if (unsigned == STRING_ESCAPE) {
                buffer.write(STRING_ESCAPED_NUL);
            }
        }
        buffer.write(STRING_ESCAPE);
        buffer.write(STRING_TERMINATOR);
    }

    /**
     * Inverts every accumulated byte, turning the ascending key into a descending one.
     *
     * <p>Mirrors {@link java.util.Comparator#reversed() Comparator.reversed()} applied to the
     * whole comparison, which is how the {@code (compare_by)} {@code descending} flag is realized.
     */
    void invertAll() {
        var bytes = buffer.toByteArray();
        buffer.reset();
        for (var b : bytes) {
            buffer.write(~b & 0xFF);
        }
    }

    /** Renders the accumulated bytes as a lowercase hex string. */
    String toHexString() {
        var bytes = buffer.toByteArray();
        var chars = new char[bytes.length * 2];
        for (var i = 0; i < bytes.length; i++) {
            var v = bytes[i] & 0xFF;
            chars[i * 2] = HEX[v >>> 4];
            chars[i * 2 + 1] = HEX[v & 0x0F];
        }
        return new String(chars);
    }

    private void putRawLong(long value) {
        for (var shift = 56; shift >= 0; shift -= 8) {
            buffer.write((int) (value >>> shift) & 0xFF);
        }
    }
}
