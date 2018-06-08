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

package io.spine.web.given;

import io.grpc.stub.StreamObserver;
import io.spine.core.Ack;
import io.spine.core.Command;
import io.spine.protobuf.AnyPacker;
import io.spine.server.CommandService;
import io.spine.web.CommandServlet;

import static io.spine.core.Responses.statusOk;
import static io.spine.test.Tests.nullRef;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Dmytro Dashenkov
 */
public final class CommandServletTestEnv {

    /**
     * Prevents the utility class instantiation.
     */
    private CommandServletTestEnv() {
    }

    @SuppressWarnings("unchecked") // Mocking generics.
    public static CommandService positiveCommandService() {
        final CommandService commandService = mock(CommandService.class);
        doAnswer(invocation -> {
            final StreamObserver<Ack> observer = invocation.getArgument(1);
            final Command command = invocation.getArgument(0);
            observer.onNext(Ack.newBuilder()
                               .setMessageId(AnyPacker.pack(command.getId()))
                               .setStatus(statusOk())
                               .build());
            observer.onCompleted();
            return nullRef();
        }).when(commandService).post(any(Command.class), any(StreamObserver.class));
        return commandService;
    }

    @SuppressWarnings("serial")
    public static final class TestCommandServlet extends CommandServlet {

        public TestCommandServlet() {
            super(positiveCommandService());
        }
    }
}
