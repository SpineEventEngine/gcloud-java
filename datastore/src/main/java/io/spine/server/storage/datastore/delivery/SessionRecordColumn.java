/*
 * Copyright 2021, TeamDev. All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import io.spine.query.RecordColumn;
import io.spine.query.RecordColumns;
import io.spine.server.delivery.ShardSessionRecord;

import static io.spine.query.RecordColumn.create;

/**
 * The definitions of record columns to store along with the {@link ShardSessionRecord}.
 *
 * @apiNote This type is made {@code public} to allow library users query the stored
 *         {@code ShardSessionRecord}s via storage API, in case they need to read
 *         the storage contents manually.
 */
@RecordColumns(ofType = ShardSessionRecord.class)
@SuppressWarnings({"BadImport" /* Using `create` API for columns for brevity.  */,
        "WeakerAccess" /* See API note. */})
public final class SessionRecordColumn {

    public static final RecordColumn<ShardSessionRecord, Integer>
            shard = create("shard", Integer.class, (r) -> r.getIndex()
                                                           .getIndex());

    public static final RecordColumn<ShardSessionRecord, Integer>
            total_shards = create("total_shards", Integer.class, (r) -> r.getIndex()
                                                                         .getOfTotal());

    public static final RecordColumn<ShardSessionRecord, String>
            worker = create("worker", String.class, (r) -> {
                var id = r.getWorker();
                var node = id.getNodeId();
                var result = node.getValue() + '-' + id.getValue();
                return result;
    });

    public static final RecordColumn<ShardSessionRecord, Timestamp>
            when_last_picked = create("when_last_picked", Timestamp.class,
                                      ShardSessionRecord::getWhenLastPicked);

    /**
     * Prevents this type from instantiation.
     *
     * <p>This class exists exclusively as a container of the column definitions. Thus, it isn't
     * expected to be instantiated. See the {@link RecordColumns} docs for more details on
     * this approach.
     */
    private SessionRecordColumn() {
    }

    /**
     * Returns the definitions of all columns.
     */
    public static ImmutableList<RecordColumn<ShardSessionRecord, ?>> definitions() {
        return ImmutableList.of(shard, total_shards, worker, when_last_picked);
    }
}
