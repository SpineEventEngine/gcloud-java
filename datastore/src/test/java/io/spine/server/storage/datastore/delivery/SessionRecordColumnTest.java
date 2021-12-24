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

import io.spine.server.NodeId;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.WorkerId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.*;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.datastore.given.TestShardIndex.newIndex;

@DisplayName("SessionRecordColumn should")
class SessionRecordColumnTest {

    @Nested
    @DisplayName("return the modified value for `worker` column when")
    class UpdateWorkerColumn {

        private final NodeId initialNode = newNodeId();
        private final WorkerId initialWorker = newWorkerId(initialNode);
        private final String initialValue = columnValueFor(initialWorker);

        @Test
        @DisplayName("node changes")
        void nodeChanges() {
            var node = newNodeId();
            var worker = initialWorker
                    .toBuilder()
                    .setNodeId(node)
                    .vBuild();
            var value = columnValueFor(worker);
            assertThat(value).isNotEqualTo(initialValue);
        }

        @Test
        @DisplayName("worker changes")
        void workerChanges() {
            var worker = newWorkerId(initialNode);
            var value = columnValueFor(worker);
            assertThat(value).isNotEqualTo(initialValue);
        }

        @Test
        @DisplayName("node and worker change")
        void nodeAndWorkerChanges() {
            var node = newNodeId();
            var worker = newWorkerId(node);
            var value = columnValueFor(worker);
            assertThat(value).isNotEqualTo(initialValue);
        }

        private NodeId newNodeId() {
            return NodeId
                    .newBuilder()
                    .setValue(newUuid())
                    .vBuild();
        }

        private WorkerId newWorkerId(NodeId node) {
            return WorkerId
                    .newBuilder()
                    .setNodeId(node)
                    .setValue(newUuid())
                    .vBuild();
        }

        @SuppressWarnings("ConstantConditions" /* We are not using a "material" record here. */)
        private String columnValueFor(WorkerId worker) {
            var session = ShardSessionRecord
                    .newBuilder()
                    .setIndex(newIndex(1, 15))
                    .setWorker(worker)
                    .vBuild();
            return SessionRecordColumn.worker.valueIn(session);
        }
    }
}
