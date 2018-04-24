/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import io.spine.core.Command;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Optional;

import static io.spine.web.parser.HttpMessages.parse;

/**
 * A parser for a {@link Command} contained in an {@link HttpServletRequest}.
 *
 * @author Dmytro Dashenkov
 */
final class CommandParser {

    private final Command command;

    private CommandParser(Command command) {
        this.command = command;
    }

    /**
     * Creates a {@code CommandParser} for the given {@link HttpServletRequest}.
     *
     * <p>The given request should contain a JSON representation of a {@link Command} in its body.
     *
     * @param request the request with a {@link Command}
     * @return the command if the given request can be parsed into a {@link Command},
     *         {@code Optional.empty()} otherwise
     */
    static Optional<CommandParser> from(HttpServletRequest request) throws IOException {
        return parse(request, Command.class).map(CommandParser::new);
    }

    /**
     * Reports the parsed {@link Command io.spine.core.Command}.
     *
     * @return an instance of {@link Command} to handle
     */
    Command command() {
        return command;
    }
}
