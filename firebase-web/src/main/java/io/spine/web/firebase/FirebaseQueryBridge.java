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

package io.spine.web.firebase;

import com.google.firebase.database.FirebaseDatabase;
import io.spine.web.QueryBridge;
import io.spine.web.QueryProcessingResult;
import io.spine.web.queryservice.AsyncQueryService;
import io.spine.client.Query;
import io.spine.client.QueryResponse;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceBlockingStub;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceImplBase;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An implementation of {@link QueryBridge} based on the Firebase Realtime Database.
 *
 * <p>This bridge stores the {@link QueryResponse} data to a location in a given
 * {@link FirebaseDatabase} and retrieves the database path to that response as the result.
 *
 * <p>More formally, for each encountered {@link Query}, the bridge performs a call to
 * the {@code QueryService} and stores the resulting entity states into the given database. The data
 * is stored as a list of strings. Each entry is
 * a {@linkplain io.spine.json.Json JSON representation} of an entity state. The path produced by
 * the bridge as a result is the path to the database node containing all those records.
 * The absolute position of such a node is not specified, thus the result path is the only way
 * to read the data from the database.
 *
 * <p>Note that the database writes are non-blocking. This means that when
 * the {@link #send(Query)} method exits, the records may or may not be in
 * the database yet.
 *
 * @author Dmytro Dashenkov
 */
public final class FirebaseQueryBridge implements QueryBridge {

    private final AsyncQueryService queryService;
    private final FirebaseDatabase database;
    private final long writeAwaitSeconds;

    private FirebaseQueryBridge(Builder builder) {
        this.queryService = builder.queryService;
        this.database = builder.database;
        this.writeAwaitSeconds = builder.writeAwaitSeconds;
    }

    /**
     * Sends the given {@link Query} to the {@link io.spine.server.QueryService QueryService} and
     * stores the query response into the database.
     *
     * <p>Returns the path in the database, under which the query response is stored.
     *
     * @param query the query to send
     * @return a path in the database
     */
    @Override
    public QueryProcessingResult send(Query query) {
        final CompletableFuture<QueryResponse> queryResponse = queryService.execute(query);
        final FirebaseRecord record = new FirebaseRecord(query, queryResponse, writeAwaitSeconds);
        record.storeTo(database);
        final QueryProcessingResult result = new FirebaseQueryProcessingResult(record.path());
        return result;
    }

    /**
     * Creates a new instance of {@code Builder} for {@code FirebaseQueryBridge} instances.
     *
     * @return new instance of {@code Builder}
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for the {@code FirebaseQueryBridge} instances.
     */
    public static final class Builder {

        /**
         * The default amount of seconds to wait for a single record to be written.
         */
        private static final long DEFAULT_WRITE_AWAIT_SECONDS = 60L;

        private AsyncQueryService queryService;
        private FirebaseDatabase database;
        private long writeAwaitSeconds = DEFAULT_WRITE_AWAIT_SECONDS;

        /**
         * Prevents local instantiation.
         */
        private Builder() {
        }

        public Builder setQueryService(QueryServiceBlockingStub service) {
            checkNotNull(service);
            this.queryService = AsyncQueryService.remote(service);
            return this;
        }

        public Builder serQueryService(QueryServiceImplBase service) {
            checkNotNull(service);
            this.queryService = AsyncQueryService.local(service);
            return this;
        }

        public Builder setDatabase(FirebaseDatabase database) {
            this.database = checkNotNull(database);
            return this;
        }

        /**
         * Sets the amount of seconds to wait for a single record to be written.
         *
         * <p>The default value is {@code 60} seconds.
         *
         * @param writeAwaitSeconds time to await a single write operation, in seconds
         */
        public Builder setWriteAwaitSeconds(long writeAwaitSeconds) {
            this.writeAwaitSeconds = writeAwaitSeconds;
            return this;
        }

        /**
         * Creates a new instance of {@code FirebaseQueryBridge}.
         *
         * @return new instance of {@code FirebaseQueryBridge}
         */
        public FirebaseQueryBridge build() {
            checkState(queryService != null, "Query Service is not set.");
            checkState(database != null, "FirebaseDatabase is not set.");
            return new FirebaseQueryBridge(this);
        }
    }
}
