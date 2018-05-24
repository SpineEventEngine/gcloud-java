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

package io.spine.web.parser;

import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import io.spine.base.Time;
import io.spine.core.Ack;
import io.spine.core.Responses;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Base64;
import java.util.Optional;

import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("HttpMessages should")
class HttpMessagesTest {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String JSON_TYPE = "application/json";

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String PROTOBUF_TYPE = "application/x-protobuf";

    @Test
    @DisplayName("have private utility ctor")
    void testUtilCtor() {
        assertHasPrivateParameterlessCtor(HttpMessages.class);
    }

    @Test
    @DisplayName("parse message from JSON in HTTP request")
    void testParseEscaped() throws IOException {
        final String content = "\"{ status: { ok: {}} }\"";
        final Ack expected = Ack.newBuilder()
                                .setStatus(Responses.statusOk())
                                .build();
        final Optional<Ack> actual = HttpMessages.parse(requestWithJson(content), Ack.class);
        assertTrue(actual.isPresent());
        assertEquals(expected, actual.get());
    }

    @Test
    @DisplayName("parse message from Base64 string in HTTP request")
    void testBase64() throws IOException {
        final Message expectedMessage = FieldMask.newBuilder()
                                                 .addPaths("Dummy.field")
                                                 .build();
        final String content = base64(expectedMessage);
        final Optional<FieldMask> actual =
                HttpMessages.parse(requestInFormat(content, PROTOBUF_TYPE),
                                   FieldMask.class);
        assertTrue(actual.isPresent());
        assertEquals(expectedMessage, actual.get());
    }

    @Test
    @DisplayName("not parse message of an unknown format")
    void testNotSupportUnknownFormat() throws IOException {
        final String content = "whatever";
        final Optional<?> result = HttpMessages.parse(requestInFormat(content, "invalid-format"),
                                                      Message.class);
        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("parse message of JSON format specified explicitly")
    void testParseExplicitJson() throws IOException {
        final String content = "{}";
        final Optional<Empty> parsed = HttpMessages.parse(requestInFormat(content, JSON_TYPE),
                                                          Empty.class);
        assertTrue(parsed.isPresent());
        assertEquals(Empty.getDefaultInstance(), parsed.get());
    }

    @Test
    @DisplayName("fail to parse a malformed byte string")
    void testFailToParseBytes() throws IOException {
        final String content = Base64.getEncoder().encodeToString(
                new byte[] {(byte) 1, (byte) 42, (byte) 127}
        );
        final Optional<?> parsed = HttpMessages.parse(requestInFormat(content, PROTOBUF_TYPE),
                                                      Empty.class);
        assertFalse(parsed.isPresent());
    }

    @Test
    @DisplayName("fail to parse wrong type of message from JSON")
    void testJsonWrongType() throws IOException {
        final String content = "{ \"foo\": \"bar\" }";
        final Optional<?> parsed = HttpMessages.parse(requestWithJson(content), Empty.class);
        assertFalse(parsed.isPresent());
    }

    @Test
    @DisplayName("parse to parse wrong type of message from bytes into unknown fields")
    void testBase64WrongType() throws IOException {
        final Timestamp message = Time.getCurrentTime();
        final String content = base64(message);
        final Optional<Empty> parsed = HttpMessages.parse(requestInFormat(content, PROTOBUF_TYPE),
                                                          Empty.class);
        assertTrue(parsed.isPresent());
        final Empty parsingResult = parsed.get();
        final UnknownFieldSet unknownFields = parsingResult.getUnknownFields();
        assertEquals(message.getSeconds(),
                     (long) unknownFields.getField(1)
                                         .getVarintList()
                                         .get(0));
        assertEquals(message.getNanos(), (long) unknownFields.getField(2)
                                                             .getVarintList()
                                                             .get(0));
    }

    private static String base64(Message message) {
        final byte[] messageBytes = message.toByteArray();
        final String result = Base64.getEncoder()
                                    .encodeToString(messageBytes);
        return result;
    }

    private static HttpServletRequest requestWithJson(String content) throws IOException {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getReader()).thenReturn(new BufferedReader(new StringReader(content)));
        return request;
    }

    private static HttpServletRequest requestInFormat(String content, String format)
            throws IOException {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getReader()).thenReturn(new BufferedReader(new StringReader(content)));
        when(request.getHeader(eq(MessageFormat.CONTENT_TYPE))).thenReturn(format);
        return request;
    }
}
