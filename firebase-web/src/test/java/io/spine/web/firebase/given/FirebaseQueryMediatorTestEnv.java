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

package io.spine.web.firebase.given;

import com.google.api.core.ApiFuture;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.grpc.stub.StreamObserver;
import io.spine.client.Query;
import io.spine.client.QueryResponse;
import io.spine.client.grpc.QueryServiceGrpc;
import io.spine.protobuf.AnyPacker;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableSet.copyOf;
import static io.spine.core.Responses.ok;
import static java.util.stream.Collectors.toSet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public final class FirebaseQueryMediatorTestEnv {

    /**
     * Prevents the utility class instantiation.
     */
    private FirebaseQueryMediatorTestEnv() {
    }

    /**
     * Produces a mock {@link ApiFuture} which thrown a {@link TimeoutException} on any call
     * to {@link java.util.concurrent.Future#get(long, TimeUnit)}.
     *
     * <p>The resulting mock does not support any other methods.
     *
     * @param <T> the type of the future
     * @return a mock future
     */
    public static <T> ApiFuture<T> timeoutFuture()
            throws InterruptedException, ExecutionException, TimeoutException {
        @SuppressWarnings("unchecked")
        final ApiFuture<T> future = mock(ApiFuture.class);

        when(future.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new TimeoutException("FirebaseQueryMediatorTestEnv.timeoutFuture()"));
        return future;
    }

    public static final class TestQueryService extends QueryServiceGrpc.QueryServiceImplBase {

        private final Collection<Any> response;

        public TestQueryService(Message... messages) {
            this.response = copyOf(messages).stream()
                                            .map(AnyPacker::pack)
                                            .collect(toSet());
        }

        @Override
        public void read(Query request, StreamObserver<QueryResponse> responseObserver) {
            final QueryResponse queryResponse = QueryResponse.newBuilder()
                                                             .setResponse(ok())
                                                             .addAllMessages(response)
                                                             .build();
            responseObserver.onNext(queryResponse);
            responseObserver.onCompleted();
        }
    }
}
