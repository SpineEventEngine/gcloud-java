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
