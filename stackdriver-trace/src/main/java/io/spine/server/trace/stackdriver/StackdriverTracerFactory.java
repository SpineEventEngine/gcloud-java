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

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ClientContext;
import com.google.cloud.trace.v2.stub.GrpcTraceServiceStub;
import io.spine.core.Signal;
import io.spine.server.ContextSpec;
import io.spine.server.trace.Tracer;
import io.spine.server.trace.TracerFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link TracerFactory} based on the Stackdriver Trace.
 *
 * <p>This tracing mechanism requires a Google Cloud Project with enabled Cloud Trace (another name
 * for Stackdriver Trace).
 *
 * @see <a href="https://cloud.google.com/trace/docs/">Stackdriver Trace docs</a>
 */
public class StackdriverTracerFactory implements TracerFactory {

    private static final String DEFAULT_ENDPOINT = "cloudtrace.googleapis.com";

    private final TraceService service;
    private final ProjectId gcpProjectId;

    private StackdriverTracerFactory(Builder builder) {
        this(builder.buildService(), builder.gcpProjectId);
    }

    @SuppressWarnings("WeakerAccess") // Allows customization via subclassing.
    protected StackdriverTracerFactory(TraceService service,
                                       ProjectId gcpProjectId) {
        this.service = checkNotNull(service);
        this.gcpProjectId = checkNotNull(gcpProjectId);
    }

    /**
     * Obtains the Cloud Stackdriver Trace endpoint for the gRPC calls.
     *
     * @return the {@code cloudtrace.googleapis.com} endpoint
     */
    public static String stackdriverEndpoint() {
        return DEFAULT_ENDPOINT;
    }

    @Override
    public Tracer trace(ContextSpec context, Signal<?, ?, ?> signalMessage) {
        return new StackdriverTracer(signalMessage, service, gcpProjectId, context.name());
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

        private ClientContext clientContext;
        private ProjectId gcpProjectId;
        private boolean multiThreadingAllowed = true;

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
        }

        /**
         * Sets the Google Cloud Platform project ID.
         *
         * <p>To find out the ID of a GCP project, go to
         * the <a href="https://console.cloud.google.com">Google Cloud Console</a> and open
         * the {@code "Select a project"} dropdown. The table of projects contains projects
         * available to you. The project ID is the value of the {@code ID} column of the table.
         *
         * <p>This field is required.
         *
         * @param gcpProjectId the Google Cloud Platform project ID
         */
        public Builder setGcpProjectId(String gcpProjectId) {
            checkNotNull(gcpProjectId);
            this.gcpProjectId = new ProjectId(gcpProjectId);
            return this;
        }

        /**
         * Sets the gRPC call context to use when submitting collected spans to Trace.
         *
         * <p>Either this field or the {@linkplain #setClientContext(ClientContext) client context}
         * must be set.
         */
        public Builder setCallContext(GrpcCallContext callContext) {
            checkNotNull(callContext);
            this.clientContext = ClientContext
                    .newBuilder()
                    .setDefaultCallContext(callContext)
                    .build();
            return this;
        }

        /**
         * Sets the Google API client context to use when submitting collected spans to Trace.
         *
         * <p>Either this field or the {@linkplain #setCallContext(GrpcCallContext)} call context}
         * must be set.
         */
        public Builder setClientContext(ClientContext context) {
            this.clientContext = checkNotNull(context);
            return this;
        }

        public Builder forbidMultiThreading() {
            this.multiThreadingAllowed = false;
            return this;
        }

        /**
         * Creates a new instance of {@code StackdriverTracerFactory}.
         *
         * @return new instance of {@code StackdriverTracerFactory}
         */
        public StackdriverTracerFactory build() {
            checkNotNull(clientContext, "Either CallContext or ClientContext must be set.");
            checkNotNull(gcpProjectId, "GCP project name must be set.");
            return new StackdriverTracerFactory(this);
        }

        private TraceService buildService() {
            GrpcTraceServiceStub service;
            try {
                service = GrpcTraceServiceStub.create(clientContext);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return multiThreadingAllowed
                   ? new AsyncTraceService(service)
                   : new SyncTraceService(service);
        }
    }
}
