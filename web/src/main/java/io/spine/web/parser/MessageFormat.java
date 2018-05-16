/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.parser;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Message formats supported by the {@link HttpMessages}.
 *
 * @author Dmytro Dashenkov
 */
enum MessageFormat {

    /**
     * The JSON message stringification format.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection") // The duplication is a coincidence.
    JSON("application/json") {
        @Override
        <M extends Message> MessageParser<M> parserFor(Class<M> type) {
            return new JsonMessageParser<>(type);
        }

        @Override
        protected boolean matches(@Nullable String requestContentType) {
            return isNullOrEmpty(requestContentType) || super.matches(requestContentType);
        }
    },

    /**
     * The Base64 bytes message stringification format.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection") // The duplication is a coincidence.
    BASE64("application/x-protobuf") {
        @Override
        <M extends Message> MessageParser<M> parserFor(Class<M> type) {
            return new Base64MessageParser<>(type);
        }
    };

    @VisibleForTesting
    static final String CONTENT_TYPE = "Content-Type";

    private final String contentType;

    MessageFormat(String contentType) {
        this.contentType = contentType;
    }

    /**
     * Finds the required format for the given {@linkplain HttpServletRequest request}.
     *
     * <p>The format is determined by the value of the {@code Content-Type} header.
     * If the value is equal to {@code application/json} (case insensitive), returns {@link #JSON}.
     * If the value is equal to {@code application/x-protobuf} (case insensitive), returns
     * {@link #BASE64}. If the header is not set, returns {@link #JSON}.
     *
     * @param request the request to get the format for
     * @return the format of the message in the given request or {@code Optional.empty()} if
     *         the request does not justify the described format
     */
    static Optional<MessageFormat> formatOf(HttpServletRequest request) {
        final String formatHeader = request.getHeader(CONTENT_TYPE);
        final Optional<MessageFormat> matchedFormat =
                Stream.of(values())
                      .filter(format -> format.matches(formatHeader))
                      .findFirst();
        return matchedFormat;
    }

    protected boolean matches(@Nullable String requestContentType) {
        final boolean result = contentType.equalsIgnoreCase(requestContentType);
        return result;
    }

    /**
     * Creates a {@link MessageParser} for the given {@code type}.
     *
     * <p>The parser works with {@code this} message format.
     *
     * @param type the class of the message to parse
     * @param <M>  the type of the message to parse
     * @return a message parses instance
     */
    abstract <M extends Message> MessageParser<M> parserFor(Class<M> type);
}
