/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web.parser;

import com.google.protobuf.Message;

import java.util.Optional;

/**
 * A string to message parser.
 *
 * <p>The string format is implementation specific.
 *
 * @param <M> the type of messages to parse
 * @author Dmytro Dashenkov
 */
interface MessageParser<M extends Message> {

    /**
     * Parses the given string into a message.
     *
     * @param raw the string to parse
     * @return parsed message or {@code Optional.empty()} if the string cannot be parsed into
     *         a message of type {@code M}
     */
    Optional<M> parse(String raw);
}
