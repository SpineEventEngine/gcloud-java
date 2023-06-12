/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.datastore.delivery;

import io.spine.query.QueryPredicate;
import io.spine.query.RecordQuery;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.datastore.config.EntityGroupLayout;
import io.spine.server.storage.datastore.record.RecordId;

import java.util.Optional;

import static io.spine.server.delivery.InboxColumn.inbox_shard;
import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * Describes the ancestor-child structure of {@link InboxMessage}s stored as Datastore entities.
 *
 * <p>{@code InboxMessage} are grouped by the indexes of shards to which they belong.
 */
public final class InboxStorageLayout
        extends EntityGroupLayout<InboxMessageId, InboxMessage, ShardIndex> {

    /**
     * Creates a new instance of the layout.
     */
    public InboxStorageLayout() {
        super(InboxMessage.class, ShardIndex.class);
    }

    @Override
    protected RecordId toAncestorRecordId(InboxMessageId id) {
        var index = id.getIndex();
        var result = RecordId.ofEntityId(index);
        return result;
    }

    @Override
    protected RecordId extractAncestorId(RecordQuery<InboxMessageId, InboxMessage> query) {
        var subject = query.subject();
        var idValues = subject.id()
                              .values();
        var sizeOfIds = idValues.size();
        if (sizeOfIds > 1) {
            throw newIllegalArgumentException(
                    "Expected a single parent IDs for an `InboxMessage` query, but got %s.",
                    sizeOfIds);
        }
        if (sizeOfIds == 1) {
            var queriedId = idValues.iterator()
                                    .next();
            return toAncestorRecordId(queriedId);
        }
        var predicate = subject.predicate();
        var referencedShard = findShardIn(predicate);
        if (referencedShard.isPresent()) {
            var value = referencedShard.get();
            var result = RecordId.ofEntityId(value);
            return result;
        }
        throw newIllegalArgumentException(
                "Cannot detect the parent ID for the query fetching `InboxMessage`s. " +
                        "Query = `%s`.", query);
    }

    @SuppressWarnings("MethodWithMultipleLoops")    /* For brevity. */
    private static Optional<ShardIndex> findShardIn(QueryPredicate<InboxMessage> predicate) {
        var parameters = predicate.allParams();
        for (var parameter : parameters) {
            var columnName = parameter.column()
                                      .name();
            if (columnName.equals(inbox_shard.name())) {
                var shard = (ShardIndex) parameter.value();
                return Optional.of(shard);
            }
        }
        for (var child : predicate.children()) {
            var maybeResult = findShardIn(child);
            if (maybeResult.isPresent()) {
                return maybeResult;
            }
        }
        return Optional.empty();
    }

    @Override
    protected RecordId asRecordId(InboxMessageId id) {
        var result = RecordId.of(id.getUuid());
        return result;
    }
}
