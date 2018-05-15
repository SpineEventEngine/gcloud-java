/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
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
import static io.spine.web.parser.HttpMessages.parse;
import static java.lang.String.format;
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

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String JSON_FORMAT = "json";

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String BYTES_FORMAT = "bytes";

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
        final Optional<Ack> actual = parse(requestWithFormat(content, JSON_FORMAT), Ack.class);
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
        final Optional<FieldMask> actual = parse(requestWithFormat(content, BYTES_FORMAT),
                                                 FieldMask.class);
        assertTrue(actual.isPresent());
        assertEquals(expectedMessage, actual.get());
    }

    @Test
    @DisplayName("not parse message of an unknown format")
    void testNotSupportUnknownFormat() throws IOException {
        final String content = "whatever";
        final Optional<?> result = parse(
                requestWithContentType(content, "invalid-format"),
                Message.class);
        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("parse message of JSON format specified in Content-Type")
    void testParseJsonFromHeader() throws IOException {
        final String content = "{}";
        final Optional<Empty> parsed = parse(requestWithContentType(content, JSON_TYPE),
                                             Empty.class);
        assertTrue(parsed.isPresent());
        assertEquals(Empty.getDefaultInstance(), parsed.get());
    }

    @Test
    @DisplayName("parse message of Base64 format specified in Content-Type")
    void testParseBytesFromHeader() throws IOException {
        final String content = base64(Empty.getDefaultInstance());
        final Optional<Empty> parsed = parse(requestWithContentType(content, PROTOBUF_TYPE),
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
        final Optional<?> parsed = parse(requestWithContentType(content, PROTOBUF_TYPE),
                                         Empty.class);
        assertFalse(parsed.isPresent());
    }

    @Test
    @DisplayName("fail to parse wrong type of message from JSON")
    void testJsonWrongType() throws IOException {
        final String content = "{ \"foo\": \"bar\" }";
        final Optional<?> parsed = parse(requestWithFormat(content, JSON_FORMAT), Empty.class);
        assertFalse(parsed.isPresent());
    }

    @Test
    @DisplayName("parse wrong type of message from bytes into unknown fields")
    void testBase64WrongType() throws IOException {
        final Timestamp message = Time.getCurrentTime();
        final String content = base64(message);
        final Optional<Empty> parsed = parse(
                requestWithFormat(content, BYTES_FORMAT),
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

    private static HttpServletRequest requestWithContentType(String content, String contentType)
            throws IOException {
        final HttpServletRequest request = requestWithBody(content);
        when(request.getHeader(eq(MessageFormat.CONTENT_TYPE))).thenReturn(contentType);
        when(request.getQueryString()).thenReturn("");
        return request;
    }

    private static HttpServletRequest requestWithFormat(String content, String format)
            throws IOException {
        final HttpServletRequest request = requestWithBody(content);
        when(request.getQueryString()).thenReturn(format("format=%s", format));
        return request;
    }

    private static HttpServletRequest requestWithBody(String content) throws IOException {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getReader()).thenReturn(new BufferedReader(new StringReader(content)));
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://test.request/"));
        return request;
    }
}
