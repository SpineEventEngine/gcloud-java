/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
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
