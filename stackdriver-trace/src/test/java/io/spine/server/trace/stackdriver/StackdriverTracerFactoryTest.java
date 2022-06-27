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

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ClientContext;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.testing.NullPointerTester;
import com.google.common.truth.StringSubject;
import com.google.common.truth.Subject;
import com.google.protobuf.Empty;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.spine.base.Identifier;
import io.spine.core.BoundedContextName;
import io.spine.core.Command;
import io.spine.core.Event;
import io.spine.core.MessageId;
import io.spine.server.ContextSpec;
import io.spine.server.trace.Tracer;
import io.spine.server.trace.stackdriver.given.CountingInterceptor;
import io.spine.system.server.EntityTypeName;
import io.spine.test.stackdriver.CreateProject;
import io.spine.testing.client.TestActorRequestFactory;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static com.google.auth.oauth2.ServiceAccountCredentials.fromStream;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.core.BoundedContextNames.assumingTests;
import static io.spine.core.Versions.zero;
import static io.spine.server.ContextSpec.singleTenant;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("StackdriverTracerFactory should")
class StackdriverTracerFactoryTest {

    private static final String REAL_GCP_PROJECT = "spine-dev";
    private static final ContextSpec SPEC =
            singleTenant(StackdriverTracerFactoryTest.class.getName());

    private GrpcCallContext realGrpcContext = null;
    private CountingInterceptor interceptor;
    private ManagedChannel channel;

    @BeforeEach
    void prepareChannel() throws IOException {
        interceptor = new CountingInterceptor();
        channel = InProcessChannelBuilder
                .forName("never to be called because of the interceptor")
                .intercept(interceptor)
                .build();
        InputStream credentialsFile = StackdriverTracerFactoryTest.class
                .getClassLoader()
                .getResourceAsStream("spine-dev.json");
        assertNotNull(credentialsFile);
        ServiceAccountCredentials credentials = fromStream(credentialsFile);
        CallCredentials callCredentials = MoreCallCredentials.from(credentials);
        CallOptions options = CallOptions.DEFAULT.withCallCredentials(callCredentials);
        realGrpcContext = GrpcCallContext.of(channel, options);
    }

    @AfterEach
    void closeChannel() {
        channel.shutdownNow();
    }

    @Test
    @DisplayName("provide gRPC endpoint")
    void endpoint() {
        String endpoint = StackdriverTracerFactory.stackdriverEndpoint();
        StringSubject assertEndpoint = assertThat(endpoint);
        assertEndpoint.isNotNull();
        assertEndpoint.isNotEmpty();
    }

    @Nested
    @DisplayName("not tolerate nulls")
    class NotNull {

        @Test
        @DisplayName("on construction")
        void builder() {
            new NullPointerTester()
                    .setDefault(GrpcCallContext.class, GrpcCallContext.createDefault())
                    .setDefault(ClientContext.class, ClientContext
                            .newBuilder()
                            .setDefaultCallContext(GrpcCallContext.createDefault())
                            .build())
                    .setDefault(BoundedContextName.class, assumingTests())
                    .testAllPublicInstanceMethods(StackdriverTracerFactory.newBuilder());
        }

        @Test
        @DisplayName("when creating tracers")
        void factory() {
            StackdriverTracerFactory factory = StackdriverTracerFactory
                    .newBuilder()
                    .setGcpProjectId(REAL_GCP_PROJECT)
                    .setCallContext(realGrpcContext)
                    .build();
            new NullPointerTester()
                    .setDefault(ContextSpec.class, singleTenant(NotNull.class.getName()))
                    .testAllPublicInstanceMethods(factory);
        }
    }

    @Nested
    @DisplayName("not be built without")
    class NotBeBuilt {

        @Test
        @DisplayName("a client context")
        void clientContext() {
            StackdriverTracerFactory.Builder builder = StackdriverTracerFactory
                    .newBuilder()
                    .setGcpProjectId("test123");
            assertThrows(NullPointerException.class, builder::build);
        }

        @Test
        @DisplayName("a GCP project ID")
        void gcpProjectId() {
            ClientContext context = ClientContext
                    .newBuilder()
                    .setDefaultCallContext(GrpcCallContext.createDefault())
                    .build();
            StackdriverTracerFactory.Builder builder = StackdriverTracerFactory
                    .newBuilder()
                    .setClientContext(context);
            assertThrows(NullPointerException.class, builder::build);
        }
    }

    @Nested
    @DisplayName("be built with")
    class BeBuilt {

        @Test
        @DisplayName("ClientContext")
        void clientContext() {
            ClientContext context = ClientContext
                    .newBuilder()
                    .setDefaultCallContext(GrpcCallContext.createDefault())
                    .build();
            StackdriverTracerFactory
                    .newBuilder()
                    .setGcpProjectId("test321")
                    .setClientContext(context)
                    .build();
        }

        @Test
        @DisplayName("CallContext")
        void callContext() {
            StackdriverTracerFactory
                    .newBuilder()
                    .setGcpProjectId("test132")
                    .setCallContext(GrpcCallContext.createDefault())
                    .build();
        }
    }

    @Nested
    @DisplayName("create a Tracer")
    class CreateTracer {

        private StackdriverTracerFactory.Builder factory;

        @BeforeEach
        void setUp() {
            factory = StackdriverTracerFactory
                    .newBuilder()
                    .setGcpProjectId(REAL_GCP_PROJECT)
                    .setCallContext(realGrpcContext);
        }

        @Test
        @DisplayName("of correct type")
        void type() {
            StackdriverTracerFactory tracerFactory = factory.build();
            Tracer tracer = tracerFactory.trace(SPEC, Event.getDefaultInstance());
            Subject assertTracer = assertThat(tracer);
            assertTracer.isNotNull();
            assertTracer.isInstanceOf(StackdriverTracer.class);
        }

        @Test
        @DisplayName("which sends requests sequentially")
        void forSyncExecution() throws Exception {
            StackdriverTracerFactory tracerFactory = factory.forbidMultiThreading()
                                                            .build();
            assertThat(interceptor.callCount()).isEqualTo(0);
            Tracer tracer = tracerFactory.trace(SPEC, Event.getDefaultInstance());
            tracer.close();
            tracerFactory.close();
            assertThat(interceptor.callCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("which sends requests in parallel")
        void forAsyncExecution() throws Exception {
            StackdriverTracerFactory tracerFactory = factory.build();
            assertThat(interceptor.callCount()).isEqualTo(0);
            Tracer tracer = tracerFactory.trace(SPEC, Command.getDefaultInstance());
            tracer.close();
            tracerFactory.close();
            assertThat(interceptor.callCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("and post non-empty requests")
        void nonEmpty() throws Exception {
            StackdriverTracerFactory tracerFactory = factory.build();
            assertThat(interceptor.callCount()).isEqualTo(0);
            TestActorRequestFactory requests =
                    new TestActorRequestFactory(StackdriverTracerFactoryTest.class);
            CreateProject command = CreateProject
                    .newBuilder()
                    .setUuid(newUuid())
                    .setName("TestProject")
                    .vBuild();
            Command cmd = requests.command()
                                  .create(command);
            Tracer tracer = tracerFactory.trace(SPEC, cmd);
            MessageId receiverId = MessageId
                    .newBuilder()
                    .setId(Identifier.pack("SampleEntityId"))
                    .setTypeUrl(TypeUrl.of(Empty.class).value())
                    .setVersion(zero())
                    .vBuild();
            EntityTypeName entityType = EntityTypeName
                    .newBuilder()
                    .setJavaClassName(StackdriverTracerFactoryTest.class.getCanonicalName())
                    .vBuild();
            tracer.processedBy(receiverId, entityType);
            tracer.close();
            tracerFactory.close();
            assertThat(interceptor.callCount()).isEqualTo(1);
        }
    }
}
