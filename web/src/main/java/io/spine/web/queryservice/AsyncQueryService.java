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

import io.spine.client.Query;
import io.spine.client.QueryResponse;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceBlockingStub;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceImplBase;

import java.util.concurrent.CompletableFuture;

/**
 * An async proxy for a {@code QueryService}.
 *
 * @author Dmytro Dashenkov
 */
public interface AsyncQueryService {

    /**
     * Executes the given {@link Query} asynchronously.
     *
     * <p>The resulting {@link CompletableFuture} is eventually completed with either
     * the {@link QueryResponse} or a query processing exception.
     *
     * @param query the {@link Query} to execute
     * @return a {@link CompletableFuture} which produces the query response on completion
     */
    CompletableFuture<QueryResponse> execute(Query query);

    /**
     * Creates a proxy for a local instance of {@code QueryService}.
     *
     * <p>The resulting instance performs no networking to connect to the query service. The given
     * instance of {@code QueryService} is used instead.
     *
     * @param serviceImpl the Java implementation of {@code QueryService}
     * @return new {@code AsyncQueryService}
     */
    static AsyncQueryService local(QueryServiceImplBase serviceImpl) {
        return new Local(serviceImpl);
    }

    /**
     * Creates a proxy for a remote instance of {@code QueryService}.
     *
     * <p>The resulting instance performs gRPC calls to connect to the query service.
     *
     * @param serviceStub the gRPC stub for a {@code QueryService}
     * @return new {@code AsyncQueryService}
     */
    static AsyncQueryService remote(QueryServiceBlockingStub serviceStub) {
        return new Remote(serviceStub);
    }
}
