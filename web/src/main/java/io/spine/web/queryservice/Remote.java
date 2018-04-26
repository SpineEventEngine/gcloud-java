/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.queryservice;

import io.spine.client.Query;
import io.spine.client.QueryResponse;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceBlockingStub;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * A {@link AsyncQueryService} which dispatches calls to a remote
 * {@link QueryServiceBlockingStub QueryService} via gRPC.
 *
 * @author Dmytro Dashenkov
 * @see AsyncQueryService#remote(QueryServiceBlockingStub) AsyncQueryService.remote(...)
 */
final class Remote implements AsyncQueryService {

    private final QueryServiceBlockingStub service;

    Remote(QueryServiceBlockingStub service) {
        this.service = service;
    }

    @Override
    public CompletableFuture<QueryResponse> execute(Query query) {
        final CompletableFuture<QueryResponse> result = supplyAsync(() -> service.read(query));
        return result;
    }

    @Override
    public String toString() {
        return "AsyncQueryService.remote(...)";
    }
}
