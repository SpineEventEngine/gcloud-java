/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
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
