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

package io.spine.web.queryservice;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import io.spine.client.Query;
import io.spine.client.QueryResponse;
import io.spine.client.grpc.QueryServiceGrpc;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceBlockingStub;
import io.spine.server.QueryService;
import io.spine.server.transport.GrpcContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.grpc.ManagedChannelBuilder.forAddress;
import static io.spine.client.ConnectionConstants.DEFAULT_CLIENT_SERVICE_PORT;
import static io.spine.test.Tests.nullRef;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("AsyncQueryService should")
@SuppressWarnings({"InnerClassMayBeStatic", "UtilityClassWithoutPrivateConstructor"})
class AsyncQueryServiceTest {

    private static final String EXIST_TEST_NAME = "exist";
    private static final String TO_STRING_TEST_NAME = "provide toString() method";
    private static final int TEST_GRPC_PORT = DEFAULT_CLIENT_SERVICE_PORT;

    @Nested
    @DisplayName("provide local instance that should")
    class LocalTest {

        @Test
        @DisplayName(EXIST_TEST_NAME)
        void testLocal() {
            final QueryService queryService = mockQueryService();
            final AsyncQueryService proxy = AsyncQueryService.local(queryService);
            assertNotNull(proxy);

            final Query query = Query.getDefaultInstance();
            proxy.execute(query);

            verify(queryService).read(eq(query), any());
        }

        @Test
        @DisplayName(TO_STRING_TEST_NAME)
        void testToString() {
            final String representation = AsyncQueryService.local(mockQueryService())
                                                           .toString();
            assertTrue(representation.contains(AsyncQueryService.class.getSimpleName()));
            // Same each time.
            assertEquals(representation,
                         AsyncQueryService.local(mockQueryService()).toString());
        }

        private QueryService mockQueryService() {
            final QueryService queryService = mock(QueryService.class);
            return queryService;
        }
    }

    @Nested
    @DisplayName("provide remote gRPC instance that should")
    class RemoteTest {

        private GrpcContainer container;
        private QueryService queryService;

        @BeforeEach
        void setUp() throws IOException {
            queryService = mock(QueryService.class);
            doAnswer(invocation -> {
                final StreamObserver<QueryResponse> observer = invocation.getArgument(1);
                observer.onNext(QueryResponse.getDefaultInstance());
                observer.onCompleted();
                return nullRef();
            }).when(queryService).read(any(), any());
            container = GrpcContainer.newBuilder()
                                     .addService(queryService)
                                     .setPort(TEST_GRPC_PORT)
                                     .build();
            container.start();
        }

        @AfterEach
        void tearDown() {
            container.shutdown();
        }

        @Test
        @DisplayName(EXIST_TEST_NAME)
        void testRemote() {
            final AsyncQueryService proxy = AsyncQueryService.remote(remoteQueryService());
            assertNotNull(proxy);

            final Query query = Query.getDefaultInstance();
            proxy.execute(query).join();

            verify(queryService).read(eq(query), any());
        }

        @Test
        @DisplayName(TO_STRING_TEST_NAME)
        void testToString() {
            final String representation = AsyncQueryService.remote(remoteQueryService())
                                                           .toString();
            assertTrue(representation.contains(AsyncQueryService.class.getSimpleName()));
            // Same each time.
            assertEquals(representation,
                         AsyncQueryService.remote(remoteQueryService()).toString());
        }

        private QueryServiceBlockingStub remoteQueryService() {
            final Channel channel = forAddress("127.0.0.1", TEST_GRPC_PORT)
                    .usePlaintext(true)
                    .directExecutor()
                    .build();
            final QueryServiceBlockingStub result = QueryServiceGrpc.newBlockingStub(channel);
            return result;
        }
    }
}
