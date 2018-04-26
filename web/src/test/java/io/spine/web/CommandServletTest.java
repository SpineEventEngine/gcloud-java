/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import io.spine.base.Time;
import io.spine.client.CommandFactory;
import io.spine.client.TestActorRequestFactory;
import io.spine.core.Ack;
import io.spine.core.Command;
import io.spine.json.Json;
import io.spine.protobuf.AnyPacker;
import io.spine.web.given.CommandServletTestEnv.TestCommandServlet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.StringWriter;

import static io.spine.core.Status.StatusCase.OK;
import static io.spine.web.given.Servlets.request;
import static io.spine.web.given.Servlets.response;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("CommandServlet should")
class CommandServletTest {

    private static final CommandFactory commandFactory =
            TestActorRequestFactory.newInstance(CommandServletTest.class)
                                   .command();

    @Test
    @DisplayName("fail to serialize")
    void testSerialize() throws IOException {
        final CommandServlet servlet = new TestCommandServlet();
        final ObjectOutputStream stream = new ObjectOutputStream(new ByteArrayOutputStream());
        assertThrows(UnsupportedOperationException.class, () -> stream.writeObject(servlet));
    }

    @Test
    @DisplayName("handle command POST requests")
    void testHandle() throws IOException {
        final CommandServlet servlet = new TestCommandServlet();
        final StringWriter response = new StringWriter();
        final Command command = commandFactory.create(Time.getCurrentTime());
        servlet.doPost(request(command), response(response));
        final Ack ack = Json.fromJson(response.toString(), Ack.class);
        assertEquals(OK, ack.getStatus().getStatusCase());
        assertEquals(command.getId(), AnyPacker.unpack(ack.getMessageId()));
    }

    @Test
    @DisplayName("respond 400 to an invalid command")
    void testInvalidCommand() throws IOException {
        final CommandServlet servlet = new TestCommandServlet();
        final HttpServletResponse response = response(new StringWriter());
        servlet.doPost(request(Time.getCurrentTime()), response);
        verify(response).sendError(400);
    }
}
