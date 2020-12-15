/*
 * Copyright 2020, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.trace.stackdriver.given;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static io.grpc.Status.Code.OK;

/**
 * A gRPC {@link ClientInterceptor} which counts down each time a call is made.
 *
 * <p>The interceptor does not propagate the call. Instead, it creates a default instance of
 * the response type, submits the instance as the response, and closes the call.
 */
public final class CountingInterceptor implements ClientInterceptor {

    private static final byte[] EMPTY_INPUT = new byte[0];
    private final AtomicInteger callCount = new AtomicInteger(0);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT>
    interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        callCount.incrementAndGet();
        RespT answer = method.getResponseMarshaller()
                             .parse(new ByteArrayInputStream(EMPTY_INPUT));
        return new NoOpClientCall<>(answer);
    }

    public int callCount() {
        return callCount.get();
    }

    private static final class NoOpClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        private final RespT answer;

        private NoOpClientCall(RespT answer) {
            this.answer = answer;
        }

        @Override
        public void start(Listener<RespT> listener, Metadata headers) {
            listener.onMessage(answer);
            listener.onClose(Status.fromCode(OK), new Metadata());
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void cancel(String message, Throwable cause) {}

        @Override
        public void halfClose() {}

        @Override
        public void sendMessage(ReqT message) {}
    }
}
