/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.parser;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.spine.protobuf.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Optional;

import static com.google.common.base.Throwables.getRootCause;

/**
 * An implementation of {@link MessageParser} which parses messages from
 * a {@code Base64}-encoded byte string.
 *
 * <p>A parsed string should contain the message of type {@code M} represented as bytes.
 *
 * @param <M> the type of messages to parse
 * @author Dmytro Dashenkov
 */
final class Base64MessageParser<M extends Message> implements MessageParser<M> {

    private final Class<M> type;

    Base64MessageParser(Class<M> type) {
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<M> parse(String raw) {
        final byte[] bytes = Base64.getDecoder().decode(raw);
        final Message.Builder builder = Messages.builderFor(type);
        try {
            @SuppressWarnings("unchecked") // Logically checked.
            final M message = (M) builder.mergeFrom(bytes)
                                         .build();
            return Optional.of(message);
        } catch (InvalidProtocolBufferException | ClassCastException e) {
            log().error("Unable to parse message of type {} from a Base64 string: `{}`",
                        type.getName(), raw, System.lineSeparator(), getRootCause(e).getMessage());
            return Optional.empty();
        }
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(Base64MessageParser.class);
    }
}
