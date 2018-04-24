/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.firebase;

import com.google.firebase.database.FirebaseDatabase;
import io.spine.web.QueryMediator;
import io.spine.web.QueryResult;
import io.spine.web.queryservice.AsyncQueryService;
import io.spine.client.Query;
import io.spine.client.QueryResponse;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceBlockingStub;
import io.spine.client.grpc.QueryServiceGrpc.QueryServiceImplBase;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A Firebase Realtime Database based implementation of {@link QueryMediator}.
 *
 * <p>This mediator stores the {@link QueryResponse} data to a location in a given
 * {@link FirebaseDatabase} and retrieves the database path to that response as the result.
 *
 * <p>More formally, for each encountered {@link Query}, the mediator performs a call to
 * the {@code QueryService} and stores the resulting entity states into the given database. The data
 * is stored as a list of strings. Each entry is
 * a {@linkplain io.spine.json.Json JSON representation} of an entity state. The path returned by
 * the mediator as a result is the path to the database node containing all those records.
 * The absolute position of such node is not specified, thus the result path is the only way to read
 * the data from the database.
 *
 * <p>Note that the database writes are non-blocking. This means that when
 * the {@link #mediate(Query)} method exits, the records may or may not be in
 * the database yet.
 *
 * @author Dmytro Dashenkov
 */
public final class FirebaseQueryMediator implements QueryMediator {

    private final AsyncQueryService queryService;
    private final FirebaseDatabase database;
    private final long writeAwaitSeconds;

    private FirebaseQueryMediator(Builder builder) {
        this.queryService = builder.queryService;
        this.database = builder.database;
        this.writeAwaitSeconds = builder.writeAwaitSeconds;
    }

    @Override
    public QueryResult mediate(Query query) {
        final CompletableFuture<QueryResponse> queryResponse = queryService.execute(query);
        final FirebaseRecord record = new FirebaseRecord(query, queryResponse, writeAwaitSeconds);
        record.storeTo(database);
        final QueryResult result = new FirebaseQueryResult(record.path());
        return result;
    }

    /**
     * Creates a new instance of {@code Builder} for {@code FirebaseQueryMediator} instances.
     *
     * @return new instance of {@code Builder}
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for the {@code FirebaseQueryMediator} instances.
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
         * Creates a new instance of {@code FirebaseQueryMediator}.
         *
         * @return new instance of {@code FirebaseQueryMediator}
         */
        public FirebaseQueryMediator build() {
            checkState(queryService != null, "Query Service is not set.");
            checkState(database != null, "FirebaseDatabase is not set.");
            return new FirebaseQueryMediator(this);
        }
    }

}
