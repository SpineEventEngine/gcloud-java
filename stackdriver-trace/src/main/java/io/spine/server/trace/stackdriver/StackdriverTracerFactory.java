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

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ClientContext;
import com.google.cloud.trace.v2.stub.GrpcTraceServiceStub;
import io.spine.core.BoundedContextName;
import io.spine.core.Signal;
import io.spine.server.trace.Tracer;
import io.spine.server.trace.TracerFactory;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public final class StackdriverTracerFactory implements TracerFactory {

    private final @Nullable BoundedContextName context;
    private final TraceService service;
    private final String gcpProjectName;

    private StackdriverTracerFactory(Builder builder) {
        this.context = builder.context;
        this.service = builder.buildService();
        this.gcpProjectName = builder.gcpProjectName;
    }

    @Override
    public Tracer trace(Signal<?, ?, ?> signalMessage) {
        return new StackdriverTracer(signalMessage, service, gcpProjectName, context);
    }

    @Override
    public void close() throws Exception {
        service.close();
    }

    /**
     * Creates a new instance of {@code Builder} for {@code StackdriverTracerFactory} instances.
     *
     * @return new instance of {@code Builder}
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for the {@code StackdriverTracerFactory} instances.
     */
    public static final class Builder {

        private @Nullable BoundedContextName context;
        private GrpcCallContext callContext;
        private String gcpProjectName;
        private @MonotonicNonNull GrpcTraceServiceStub service;
        private boolean multiThreadingAllowed = true;

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
        }

        public Builder setContext(@Nullable BoundedContextName context) {
            this.context = context;
            return this;
        }

        private Builder setService(GrpcTraceServiceStub service) {
            this.service = checkNotNull(service);
            return this;
        }

        public Builder setCallContext(GrpcCallContext callContext) {
            this.callContext = checkNotNull(callContext);
            return this;
        }

        public Builder setGcpProjectName(String gcpProjectName) {
            this.gcpProjectName = checkNotNull(gcpProjectName);
            return this;
        }

        public Builder forbidMultithreading() {
            this.multiThreadingAllowed = false;
            return this;
        }

        /**
         * Creates a new instance of {@code StackdriverTracerFactory}.
         *
         * @return new instance of {@code StackdriverTracerFactory}
         */
        public StackdriverTracerFactory build() {
            checkNotNull(service);
            checkNotNull(callContext);
            checkNotNull(gcpProjectName);
            return new StackdriverTracerFactory(this);
        }

        private TraceService buildService() {
            ClientContext clientContext = ClientContext
                    .newBuilder()
                    .setDefaultCallContext(callContext)
                    .build();
            try {
                this.service = GrpcTraceServiceStub.create(clientContext);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return multiThreadingAllowed
                   ? new AsyncTraceService(service)
                   : new SyncTraceService(service);
        }
    }
}
