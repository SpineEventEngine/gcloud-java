/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.parser;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
            JSON("json", "application/json") {
        @Override
        <M extends Message> MessageParser<M> parserFor(Class<M> type) {
            return new JsonMessageParser<>(type);
        }
    },

    /**
     * The Base64 bytes message stringification format.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection") // The duplication is a coincidence.
            BASE64("bytes", "application/x-protobuf") {
        @Override
        <M extends Message> MessageParser<M> parserFor(Class<M> type) {
            return new Base64MessageParser<>(type);
        }
    };

    @VisibleForTesting
    static final String CONTENT_TYPE = "Content-Type";

    private static final Pattern TYPE_PARAMETER_PATTERN = Pattern.compile(".*\\bformat=(\\w+).*");

    private final String contentType;
    private final String shortTypeName;

    MessageFormat(String shortTypeName, String contentType) {
        this.contentType = contentType;
        this.shortTypeName = shortTypeName;
    }

    /**
     * Finds the required format for the given {@linkplain HttpServletRequest request}.
     *
     * <p>The format is determined by the {@code format} parameter in the request query string.
     * If the value is equal to {@code json} (case insensitive), returns {@link #JSON}.
     * If the value is equal to {@code bytes} (case insensitive), returns {@link #BASE64}.
     *
     * <p>If the parameter is not set, tries to determine the format by the {@code Content-Type}
     * header. The valid values are {@code application/json} and {@code application/x-protobuf}
     * (for the Base64) representation.
     *
     * <p>If neither the query parameter nor the header are set to one of the expected values,
     * returns {@code Optional.empty()}.
     *
     * @param request the request to get the format for
     * @return the format of the message in the given request or {@code Optional.empty()} if
     *         the request does not justify the described format
     */
    static Optional<MessageFormat> formatOf(HttpServletRequest request) {
        final Optional<MessageFormat> fromQuery = byQuery(request);
        if (fromQuery.isPresent()) {
            return fromQuery;
        }
        final Optional<MessageFormat> fromHeader = byHeader(request);
        if (!fromHeader.isPresent()) {
            log().warn("Format of request {} is unknown.", request.getRequestURL()
                                                                  .toString());
        }
        return fromHeader;
    }

    private static Optional<MessageFormat> byQuery(HttpServletRequest request) {
        final String queryString = request.getQueryString();
        if (queryString == null) {
            return Optional.empty();
        } else {
            final String formatParameter = parseTypeParameter(request.getQueryString());
            final Optional<MessageFormat> matchedFormat =
                    Stream.of(values())
                          .filter(format -> format.matchesQueryParameter(formatParameter))
                          .findFirst();
            return matchedFormat;
        }
    }

    private static Optional<MessageFormat> byHeader(HttpServletRequest request) {
        final String contentType = request.getHeader(CONTENT_TYPE);
        final Optional<MessageFormat> matchedFormat =
                Stream.of(values())
                      .filter(format -> format.matchesHeader(contentType))
                      .findFirst();
        return matchedFormat;
    }

    private static String parseTypeParameter(String queryString) {
        final Matcher typeMatcher = TYPE_PARAMETER_PATTERN.matcher(queryString);
        final String result = typeMatcher.find() ? typeMatcher.group(1) : "";
        return result;
    }

    private boolean matchesHeader(String requestContentType) {
        final boolean result = contentType.equalsIgnoreCase(requestContentType);
        return result;
    }

    private boolean matchesQueryParameter(String type) {
        final boolean result = shortTypeName.equalsIgnoreCase(type);
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

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(MessageFormat.class);
    }
}
