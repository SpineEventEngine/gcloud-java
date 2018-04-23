/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.firebase;

import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.protobuf.Message;
import io.spine.client.Query;
import io.spine.client.QueryResponse;
import io.spine.json.Json;
import io.spine.protobuf.AnyPacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A record which can be stored into a {@link FirebaseDatabase}.
 *
 * <p>A single record represents a {@linkplain QueryResponse response to a single query}.
 *
 * @author Dmytro Dashenkov
 */
final class FirebaseRecord {

    private final FirebaseDatabasePath path;
    private final CompletionStage<QueryResponse> queryResponse;
    private final long writeAwaitSeconds;

    FirebaseRecord(Query query,
                   CompletionStage<QueryResponse> queryResponse,
                   long writeAwaitSeconds) {
        this.path = FirebaseDatabasePath.allocateForQuery(query);
        this.queryResponse = queryResponse;
        this.writeAwaitSeconds = writeAwaitSeconds;
    }

    /**
     * Retrieves the database path to this record.
     */
    FirebaseDatabasePath path() {
        return path;
    }

    /**
     * Writes this record to the given {@link FirebaseDatabase}.
     *
     * @see FirebaseQueryMediator FirebaseQueryMediator for the detailed storage protocol
     */
    void storeTo(FirebaseDatabase database) {
        final DatabaseReference reference = path().reference(database);
        flushTo(reference);
    }

    private void flushTo(DatabaseReference reference) {
        queryResponse.thenAccept(response -> response.getMessagesList()
                                                     .parallelStream()
                                                     .unordered()
                                                     .map(AnyPacker::<Message>unpack)
                                                     .map(Json::toCompactJson)
                                                     .map(value -> reference.push()
                                                                            .setValueAsync(value))
                                                     .forEach(this::mute));
    }

    /**
     * Awaits the given {@link Future} and catches all the exceptions.
     *
     * <p>The encountered exceptions are logged and never thrown.
     */
    private void mute(Future<?> future) {
        try {
            future.get(writeAwaitSeconds, SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log().error(e.getMessage());
        }
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(FirebaseRecord.class);
    }
}
