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
import io.spine.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Throwables.getRootCause;
import static java.util.regex.Pattern.LITERAL;
import static java.util.regex.Pattern.compile;

/**
 * An implementation of {@link MessageParser} which parses messages from their JSON representations.
 *
 * <p>See {@link Json} and the
 * <a href="https://developers.google.com/protocol-buffers/docs/proto3#json">Protobuf documentation
 * </a> for the detailed description of the message format.
 *
 * @param <M> the type of messages to parse
 * @author Dmytro Dashenkov
 */
final class JsonMessageParser<M extends Message> implements MessageParser<M> {

    private final Class<M> type;

    JsonMessageParser(Class<M> type) {
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<M> parse(String raw) {
        final String json = cleanUp(raw);
        try {
            final M message = Json.fromJson(json, type);
            return Optional.of(message);
        } catch (IllegalArgumentException e) {
            log().error("Unable to parse message of type {} from JSON: `{}`",
                        type.getName(), json, System.lineSeparator(), getRootCause(e).getMessage());
            return Optional.empty();
        }
    }

    private static String cleanUp(String jsonFromRequest) {
        final String json = EscapeSymbol.unEscapeAll(jsonFromRequest);
        final String unQuoted = unQuote(json);
        return unQuoted;
    }

    private static String unQuote(String json) {
        int beginIndex = 0;
        int endIndex = json.length();
        if (json.startsWith("\"")) {
            beginIndex = 1;
        }
        if (json.endsWith("\"")) {
            endIndex = json.length() - 1;
        }
        final String result = json.substring(beginIndex, endIndex);
        return result;
    }

    /**
     * An enumeration of special characters that may be escaped when sending a string over HTTP.
     */
    private enum EscapeSymbol {

        @SuppressWarnings({
                "HardcodedLineSeparator", // Work only with literal "\n"s
                "unused"                  // Used via `values()`.
        })
        LINE_FEED("\\n", "\n"),
        @SuppressWarnings("unused") // Used via `values()`.
        QUOTATION_MARK("\\\"", "\"");

        private final Pattern escapedPattern;
        private final String raw;

        EscapeSymbol(String escaped, String raw) {
            this.escapedPattern = compile(escaped, LITERAL);
            this.raw = raw;
        }

        private String unEscape(String escaped) {
            final Matcher matcher = escapedPattern.matcher(escaped);
            final String unescaped = matcher.replaceAll(raw);
            return unescaped;
        }

        private static String unEscapeAll(String escaped) {
            String json = escaped;
            for (EscapeSymbol symbol : EscapeSymbol.values()) {
                json = symbol.unEscape(json);
            }
            return json;
        }
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JsonMessageParser.class);
    }
}
