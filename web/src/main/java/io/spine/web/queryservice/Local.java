/*
 * Copyright (c) 2000-2018 TeamDev. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.queryservice;

import io.spine.web.FutureObserver;
import io.spine.client.Query;
import io.spine.client.QueryResponse;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceImplBase;

import java.util.concurrent.CompletableFuture;

/**
 * An {@link AsyncQueryService} which dispatches calls to a local
 * {@link QueryServiceImplBase QueryService}.
 *
 * @author Dmytro Dashenkov
 * @see AsyncQueryService#local(QueryServiceImplBase) AsyncQueryService.local(...)
 */
final class Local implements AsyncQueryService {

    private final QueryServiceImplBase service;

    Local(QueryServiceImplBase service) {
        this.service = service;
    }

    @Override
    public CompletableFuture<QueryResponse> execute(Query query) {
        final FutureObserver<QueryResponse> observer =
                FutureObserver.withDefault(QueryResponse.getDefaultInstance());
        service.read(query, observer);
        return observer.toFuture();
    }

    @Override
    public String toString() {
        return "AsyncQueryService.local(...)";
    }
}
