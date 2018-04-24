/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import io.spine.core.Ack;
import io.spine.core.Command;
import io.spine.json.Json;
import io.spine.server.CommandService;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Exceptions.unsupported;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;

/**
 * An {@link HttpServlet} representing the License Server command endpoint.
 *
 * <p>Handles {@code POST} requests with {@linkplain Command commands} in their bodies.
 *
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("serial") // Java serialization is not supported.
public abstract class CommandServlet extends NonSerializableServlet {

    @SuppressWarnings("DuplicateStringLiteralInspection") // The duplication is a coincidence.
    private static final String MIME_TYPE = "application/json";

    private final CommandService commandService;

    protected CommandServlet(CommandService commandService) {
        super();
        this.commandService = checkNotNull(commandService);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        final Optional<CommandParser> parsed = CommandParser.from(req);
        if (!parsed.isPresent()) {
            resp.sendError(SC_BAD_REQUEST);
        } else {
            final Command command = parsed.get().command();
            final FutureObserver<Ack> ack = FutureObserver.withDefault(Ack.getDefaultInstance());
            commandService.post(command, ack);
            final Ack result = ack.toFuture().join();
            writeToResponse(result, resp);
        }
    }

    private static void writeToResponse(Ack ack, HttpServletResponse response)
            throws IOException {
        final String json = Json.toCompactJson(ack);
        response.getWriter().append(json);
        response.setContentType(MIME_TYPE);
    }
}
