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

import com.google.protobuf.Message;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.joining;

/**
 * A HTTP request to message parser.
 *
 * <p>The parser supports two string representation formats:
 * <ul>
 *     <li>JSON - <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">
 *         the Protobuf JSON format</a>;
 *     <li>Base64 - the message bytes encoded in Base64.
 * </ul>
 *
 * <p>In order to specify either format, add the {@code Content-Type} header. The accepted values
 * are {@code application/json} and {@code application/x-protobuf}, case insensitive. If the header
 * is absent, the JSON format is expected. If the header value is not recognized, the parsing is
 * failed.
 *
 * <p>There is a difference in behavior when parsing one or the other format.
 * When parsing a JSON-encoded message, if an unknown field is found, the parsing is considered
 * failed. In contrary, an unknown field in a Base64-encoded message does not fail parsing;
 * the field can be found in the {@linkplain Message#getUnknownFields() unknown fields set} of
 * the parsed message.
 *
 * @see MessageFormat
 * @author Dmytro Dashenkov
 */
public final class HttpMessages {

    /**
     * Prevents the utility class instantiation.
     */
    private HttpMessages() {
    }

    /**
     * Parses the body of the given request into a message of the given type.
     *
     * @param request the request with a JSON in its body
     * @param type    the class of message contained in the JSON
     * @param <M>     the type of the message to parse
     * @return parsed message or {@code Optional.empty()} if the message cannot be parsed
     * @throws IOException if the {@code request} throws the exception
     */
    public static <M extends Message> Optional<M> parse(HttpServletRequest request, Class<M> type)
            throws IOException {
        checkNotNull(request, type);
        final Optional<MessageFormat> format = MessageFormat.formatOf(request);
        final String requestBody = body(request);
        final Optional<M> message = format.map(messageFormat -> messageFormat.parserFor(type))
                                          .flatMap(parser -> parser.parse(requestBody));
        return message;
    }

    private static String body(ServletRequest request) throws IOException {
        final String result = request.getReader()
                                     .lines()
                                     .collect(joining(" "));
        return result;
    }
}
