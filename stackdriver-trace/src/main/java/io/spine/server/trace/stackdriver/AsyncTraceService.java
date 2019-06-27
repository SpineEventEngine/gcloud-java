/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.trace.stackdriver;

import com.google.cloud.trace.v2.stub.GrpcTraceServiceStub;
import com.google.devtools.cloudtrace.v2.BatchWriteSpansRequest;
import io.spine.logging.Logging;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * An async implementation of the {@link TraceService}.
 *
 * <p>Logs when the request is executed successfully.
 */
final class AsyncTraceService implements TraceService, Logging {

    private final GrpcTraceServiceStub client;

    AsyncTraceService(GrpcTraceServiceStub client) {
        this.client = checkNotNull(client);
    }

    @Override
    public void writeSpans(BatchWriteSpansRequest request) {
        client.batchWriteSpansCallable()
              .futureCall(request)
              .addListener(() -> log().debug("Submitted {} spans", request.getSpansCount()),
                           directExecutor());
    }

    @Override
    public void close() {
        client.close();
    }
}
