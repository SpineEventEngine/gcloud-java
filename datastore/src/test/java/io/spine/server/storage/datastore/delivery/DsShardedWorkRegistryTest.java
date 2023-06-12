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

import com.google.common.testing.NullPointerTester;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.util.Timestamps;
import io.spine.server.NodeId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;
import io.spine.server.delivery.ShardedWorkRegistryTest;
import io.spine.server.delivery.WorkerId;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static io.spine.base.Identifier.*;
import static io.spine.server.ContextSpec.singleTenant;
import static io.spine.server.storage.datastore.given.TestShardIndex.newIndex;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@DisplayName("`DsShardedWorkRegistry` should")
final class DsShardedWorkRegistryTest extends ShardedWorkRegistryTest {

    private static final ShardIndex index = newIndex(1, 15);
    private static final NodeId node = newNode();

    private final TestDatastoreStorageFactory factory = TestDatastoreStorageFactory.local();
    private DsShardedWorkRegistry registry;

    @BeforeEach
    void setUp() {
        factory.setUp();
        var context = singleTenant(DsShardedWorkRegistryTest.class.getName());
        registry = new DsShardedWorkRegistry(factory, context);
    }

    @AfterEach
    void tearDown() {
        factory.tearDown();
    }

    @Override
    protected ShardedWorkRegistry registry() {
        return registry;
    }

    @Nested
    @DisplayName("pick up")
    class PickUp {

        @Test
        @DisplayName("a shard and write the corresponding record to the storage")
        void pickUp() {
            assertPickUp(index);
        }

        @Test
        @DisplayName("shards from different threads within the same node using different worker IDs")
        void pickUpFromDifferentThreads() throws InterruptedException {
            var firstWorker = new AtomicReference<WorkerId>();
            var firstPickUp = new Thread(() -> {
                var index = newIndex(1, 15);
                var record = assertPickUp(index);
                firstWorker.set(record.getWorker());
            });
            firstPickUp.start();

            var secondWorker = new AtomicReference<WorkerId>();
            var secondPickUp = new Thread(() -> {
                var index = newIndex(2, 15);
                var record = assertPickUp(index);
                secondWorker.set(record.getWorker());
            });
            secondPickUp.start();

            firstPickUp.join();
            secondPickUp.join();

            assertNotEquals(
                    firstWorker.get(),
                    secondWorker.get()
            );
        }

        @CanIgnoreReturnValue
        private ShardSessionRecord assertPickUp(ShardIndex index) {
            var session = registry.pickUp(index, node);
            assertThat(session).isPresent();
            assertThat(session.get().shardIndex()).isEqualTo(index);

            var record = readSingleRecord(index);
            assertThat(record.getIndex()).isEqualTo(index);
            assertThat(record.getWorker().getNodeId()).isEqualTo(node);

            return record;
        }
    }

    @Test
    @DisplayName("not be able to pick up the shard if it's already picked up")
    void cannotPickUpIfTaken() {
        var session = registry.pickUp(index, node);
        assertThat(session).isPresent();

        var sameIdxSameNode = registry.pickUp(index, node);
        assertThat(sameIdxSameNode).isEmpty();

        var sameIdxAnotherNode = registry.pickUp(index, newNode());
        assertThat(sameIdxAnotherNode).isEmpty();

        var anotherIdx = newIndex(24, 100);
        var anotherIdxSameNode = registry.pickUp(anotherIdx, node);
        assertThat(anotherIdxSameNode).isPresent();

        var anotherIdxAnotherNode = registry.pickUp(anotherIdx, newNode());
        assertThat(anotherIdxAnotherNode).isEmpty();
    }

    @Test
    @DisplayName("complete the shard session (once a worker assigned) and make it available for picking up")
    void completeSessionAndMakeItAvailable() {
        var optional = registry.pickUp(index, node);
        assertThat(optional).isPresent();

        var whenPickedFirst = readSingleRecord(index).getWhenLastPicked();
        var session = (DsShardProcessingSession) optional.get();
        session.complete();

        var completedRecord = readSingleRecord(index);
        assertThat(completedRecord.hasWorker()).isFalse();

        var anotherNode = newNode();
        var anotherOptional = registry.pickUp(index, anotherNode);
        assertThat(anotherOptional).isPresent();

        var secondSessionRecord = readSingleRecord(index);
        assertThat(secondSessionRecord.getWorker().getNodeId()).isEqualTo(anotherNode);

        var whenPickedSecond = secondSessionRecord.getWhenLastPicked();
        assertThat(Timestamps.compare(whenPickedFirst, whenPickedSecond)).isLessThan(0);
    }

    @Test
    @DisplayName("not accept `null` values in public API methods")
    void notAcceptNulls() {
        new NullPointerTester()
                .setDefault(NodeId.class, newNode())
                .setDefault(ShardIndex.class, newIndex(4, 5))
                .setDefault(ShardSessionRecord.class, ShardSessionRecord.getDefaultInstance())
                .testAllPublicInstanceMethods(registry);
    }

    private ShardSessionRecord readSingleRecord(ShardIndex index) {
        var record = registry.storage().read(index);
        assertThat(record).isPresent();
        return record.get();
    }

    private static NodeId newNode() {
        return NodeId.newBuilder()
                .setValue(newUuid())
                .build();
    }
}
