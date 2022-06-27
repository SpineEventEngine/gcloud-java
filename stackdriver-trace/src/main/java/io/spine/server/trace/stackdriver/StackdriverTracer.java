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

package io.spine.server.trace.stackdriver;

import com.google.devtools.cloudtrace.v2.BatchWriteSpansRequest;
import com.google.devtools.cloudtrace.v2.Span;
import io.spine.core.BoundedContextName;
import io.spine.core.MessageId;
import io.spine.core.Signal;
import io.spine.server.trace.AbstractTracer;
import io.spine.system.server.EntityTypeName;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.synchronizedList;

/**
 * A {@link io.spine.server.trace.Tracer} which reports to Stackdriver Trace.
 */
@SuppressWarnings("WeakerAccess") // Allows customization via subclassing.
public class StackdriverTracer extends AbstractTracer {

    private final List<Span> spans;
    private final TraceService traceService;

    private final ProjectId projectId;
    private final BoundedContextName context;

    protected StackdriverTracer(Signal<?, ?, ?> signal,
                                TraceService traceService,
                                ProjectId gcpProjectId,
                                BoundedContextName context) {
        super(signal);
        this.traceService = checkNotNull(traceService);
        this.projectId = checkNotNull(gcpProjectId);
        this.context = checkNotNull(context);
        this.spans = synchronizedList(new ArrayList<>());
    }

    @Override
    public void processedBy(MessageId receiver, EntityTypeName receiverType) {
        SignalSpan signalSpan = SignalSpan
                .newBuilder()
                .setContext(context)
                .setSignal(signal())
                .setReceiver(receiver)
                .setReceiverType(receiverType)
                .build();
        Span span = signalSpan.asTraceSpan(projectId);
        spans.add(span);
    }

    @Override
    public void close() {
        BatchWriteSpansRequest request = BatchWriteSpansRequest
                .newBuilder()
                .setName(projectId.asName().value())
                .addAllSpans(spans)
                .build();
        traceService.writeSpans(request);
    }
}
