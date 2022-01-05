/*
 * Copyright 2022, TeamDev. All rights reserved.
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

import com.google.devtools.cloudtrace.v2.Span;
import io.spine.base.Time;
import io.spine.code.java.ClassName;
import io.spine.core.BoundedContextName;
import io.spine.core.MessageId;
import io.spine.core.Signal;
import io.spine.core.SignalId;
import io.spine.system.server.EntityTypeName;

import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.protobuf.AnyPacker.unpack;
import static java.lang.String.format;

/**
 * A span based on a signal.
 *
 * <p>Signal messages may be processed once or many times by different entities. The processing time
 * is tracked and represented with a span so that each message handler invocation is converted into
 * a single span.
 *
 * <p>The exception from the span-per-handler rule is an event applier invocation, which is treated
 * as a part of the respective command handler or event reactor.
 */
@SuppressWarnings("WeakerAccess") // Allows customization via subclassing.
public class SignalSpan {

    private static final int SPAN_DISPLAY_NAME_LENGTH = 128;

    private final BoundedContextName context;
    private final Signal<?, ?, ?> signal;
    private final MessageId receiver;
    private final EntityTypeName receiverType;

    /**
     * Creates a new instance of {@code SignalSpan}.
     *
     * @param context
     *         the name of the context in which the signal in handled
     * @param signal
     *         the handled signal
     * @param receiver
     *         the ID of the handling entity
     * @param receiverType
     *         the type of the handling entity
     */
    protected SignalSpan(BoundedContextName context,
                         Signal<?, ?, ?> signal,
                         MessageId receiver,
                         EntityTypeName receiverType) {
        this.context = checkNotNull(context);
        this.signal = checkNotNull(signal);
        this.receiver = checkNotNull(receiver);
        this.receiverType = checkNotNull(receiverType);
    }

    private SignalSpan(Builder builder) {
        this(builder.context, builder.signal, builder.receiver, builder.receiverType);
    }

    /**
     * Creates a {@link Span Stackdriver Trace Span} from this signal span.
     *
     * @param gcpProjectId
     *         the Google Cloud Platform project ID
     * @return new span
     */
    protected Span asTraceSpan(ProjectId gcpProjectId) {
        var span = buildSpan(gcpProjectId);
        buildSpanAttributes(span);
        return span.build();
    }

    private String displayName() {
        var signalType = signal.enclosedTypeUrl()
                               .toTypeName();
        var className = ClassName.of(receiverType.getJavaClassName());
        return format("%s handles %s", className.toSimple(), signalType.simpleName());
    }

    private Span.Builder buildSpan(ProjectId projectId) {
        var spanId = SpanId.random();
        var whenStarted = signal.timestamp();
        var whenFinished = Time.currentTime();
        var displayName = Truncate.stringTo(displayName(), SPAN_DISPLAY_NAME_LENGTH);
        return Span.newBuilder()
                .setName(spanName(projectId, spanId).value())
                .setSpanId(spanId.value())
                .setDisplayName(displayName)
                .setStartTime(whenStarted)
                .setEndTime(whenFinished);
    }

    private void buildSpanAttributes(Span.Builder span) {
        var attributesBuilder = span.getAttributesBuilder();
        Stream.of(SpanAttribute.values())
              .forEach(attribute -> {
                  var key = attribute.qualifiedName();
                  var value = attribute.value(this);
                  attributesBuilder.putAttributeMap(key, value);
              });
    }

    private SpanName spanName(ProjectId projectId, SpanId spanId) {
        var id = signal.rootMessage()
                       .getId();
        var signalId = (SignalId) unpack(id);
        var traceId = new TraceId(signalId);
        return SpanName.from(projectId, traceId, spanId);
    }

    /**
     * Obtains the name of the bounded context to which the signal receiver belongs.
     */
    protected BoundedContextName contextName() {
        return context;
    }

    /**
     * Obtains the processed signal.
     */
    protected Signal<?, ?, ?> signal() {
        return signal;
    }

    /**
     * Obtains the signal receiver ID.
     */
    protected MessageId receiver() {
        return receiver;
    }

    /**
     * Creates a new instance of {@code Builder} for {@code SignalSpan} instances.
     *
     * @return new instance of {@code Builder}
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for the {@code SignalSpan} instances.
     */
    public static final class Builder {

        private BoundedContextName context;
        private Signal<?, ?, ?> signal;
        private MessageId receiver;
        private EntityTypeName receiverType;

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
        }

        public Builder setContext(BoundedContextName context) {
            this.context = checkNotNull(context);
            return this;
        }

        public Builder setSignal(Signal<?, ?, ?> signal) {
            this.signal = checkNotNull(signal);
            return this;
        }

        public Builder setReceiver(MessageId receiver) {
            this.receiver = checkNotNull(receiver);
            return this;
        }

        public Builder setReceiverType(EntityTypeName receiverType) {
            this.receiverType = checkNotNull(receiverType);
            return this;
        }

        /**
         * Creates a new instance of {@code SignalSpan}.
         *
         * @return new instance of {@code SignalSpan}
         */
        public SignalSpan build() {
            return new SignalSpan(this);
        }
    }
}
