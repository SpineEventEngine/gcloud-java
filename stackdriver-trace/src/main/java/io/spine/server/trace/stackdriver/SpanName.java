/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The formal name of a span as recognized by the Stackdriver Trace API.
 *
 * <p>The name consist of the Google Cloud Platform project name, the trace ID and the span ID.
 *
 * @see <a href="https://cloud.google.com/trace/docs/reference/v2/rpc/google.devtools.cloudtrace.v2#google.devtools.cloudtrace.v2.BatchWriteSpansRequest">API doc</a>
 */
final class SpanName extends TraceApiString {

    private static final long serialVersionUID = 0L;

    private SpanName(ProjectName projectName, TraceId traceId, SpanId spanId) {
        super("%s/traces/%s/spans/%s", projectName, traceId, spanId);
    }

    static SpanName from(ProjectId projectId, TraceId traceId, SpanId spanId) {
        checkNotNull(projectId);
        checkNotNull(traceId);
        checkNotNull(spanId);
        return new SpanName(projectId.asName(), traceId, spanId);
    }
}
