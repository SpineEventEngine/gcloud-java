/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore;

import com.google.common.testing.NullPointerTester;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.base.Identifier;
import io.spine.server.NodeId;
import io.spine.server.delivery.PickUpOutcome;
import io.spine.server.delivery.ShardAlreadyPickedUp;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;
import io.spine.server.delivery.ShardedWorkRegistryTest;
import io.spine.server.delivery.WorkerId;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static io.spine.server.storage.datastore.given.TestShardIndex.newIndex;

@DisplayName("`DsShardedWorkRegistry` should")
class DsShardedWorkRegistryTest extends ShardedWorkRegistryTest {

    private static final ShardIndex index = newIndex(1, 15);

    private static final NodeId nodeId = newNode();

    private final TestDatastoreStorageFactory factory =
            TestDatastoreStorageFactory.local();

    private DsShardedWorkRegistry registry;

    @BeforeEach
    void setUp() {
        factory.setUp();
        registry = new DsShardedWorkRegistry(factory);
    }

    @AfterEach
    void tearDown() {
        factory.tearDown();
    }

    @Override
    protected ShardedWorkRegistry registry() {
        return registry;
    }

    @Test
    @DisplayName("pick up the shard and write a corresponding record to the storage")
    void pickUp() {
        PickUpOutcome outcome = registry.pickUp(index, nodeId);
        WorkerId expectedWorker = registry.currentWorkerFor(nodeId);
        assertThat(outcome.hasSession()).isTrue();
        assertThat(outcome.getSession()
                          .getIndex()).isEqualTo(index);

        ShardSessionRecord record = readSingleRecord(index);
        assertThat(record.getIndex()).isEqualTo(index);
        assertThat(record.getWorker()).isEqualTo(expectedWorker);
    }

    @Test
    @DisplayName("not be able to pick up the shard if it's already picked up")
    void cannotPickUpIfTaken() {

        PickUpOutcome outcome = registry.pickUp(index, nodeId);
        assertThat(outcome.hasSession()).isTrue();
        ShardSessionRecord session = outcome.getSession();

        PickUpOutcome sameIdxSameNode = registry.pickUp(index, nodeId);
        assertThat(sameIdxSameNode.hasSession()).isFalse();
        assertThat(sameIdxSameNode.hasAlreadyPicked()).isTrue();

        ShardAlreadyPickedUp alreadyPicked = sameIdxSameNode.getAlreadyPicked();
        assertThat(alreadyPicked.getWorker()).isEqualTo(session.getWorker());

        PickUpOutcome sameIdxAnotherNode = registry.pickUp(index, newNode());
        assertThat(sameIdxAnotherNode.hasSession()).isFalse();
        assertThat(sameIdxAnotherNode.hasAlreadyPicked()).isTrue();

        ShardAlreadyPickedUp anotherAlreadyPicked = sameIdxAnotherNode.getAlreadyPicked();
        assertThat(anotherAlreadyPicked.getWorker()).isEqualTo(session.getWorker());

        ShardIndex anotherIdx = newIndex(24, 100);
        PickUpOutcome anotherIdxSameNode = registry.pickUp(anotherIdx, nodeId);
        assertThat(anotherIdxSameNode.hasSession()).isTrue();
        ShardSessionRecord anotherSession = anotherIdxSameNode.getSession();

        PickUpOutcome anotherIdxAnotherNode = registry.pickUp(anotherIdx, newNode());
        assertThat(anotherIdxAnotherNode.hasSession()).isFalse();
        assertThat(anotherIdxAnotherNode.hasAlreadyPicked()).isTrue();

        ShardAlreadyPickedUp oneMoreAnotherPicked = anotherIdxAnotherNode.getAlreadyPicked();
        assertThat(oneMoreAnotherPicked.getWorker()).isEqualTo(anotherSession.getWorker());
    }

    @Test
    @DisplayName("complete the shard session (once picked up) and make it available for picking up")
    void completeSessionAndMakeItAvailable() {
        PickUpOutcome outcome = registry.pickUp(index, nodeId);
        assertThat(outcome.hasSession()).isTrue();

        Timestamp whenPickedFirst = readSingleRecord(index).getWhenLastPicked();

        registry.release(outcome.getSession());

        ShardSessionRecord completedRecord = readSingleRecord(index);
        assertThat(completedRecord.hasWorker()).isFalse();

        NodeId anotherNode = newNode();
        WorkerId anotherWorker = registry.currentWorkerFor(anotherNode);
        PickUpOutcome anotherOutcome = registry.pickUp(index, anotherNode);
        assertThat(anotherOutcome.hasSession()).isTrue();

        ShardSessionRecord secondSessionRecord = readSingleRecord(index);
        assertThat(secondSessionRecord.getWorker()).isEqualTo(anotherWorker);

        Timestamp whenPickedSecond = secondSessionRecord.getWhenLastPicked();
        assertThat(Timestamps.compare(whenPickedFirst, whenPickedSecond) < 0).isTrue();
    }

    @Test
    @DisplayName("not accept `null` values in public API methods")
    void notAcceptNulls() {
        new NullPointerTester()
                .setDefault(NodeId.class, newNode())
                .setDefault(ShardIndex.class, newIndex(4, 5))
                .setDefault(ShardSessionRecord.class, ShardSessionRecord.getDefaultInstance())
                .setDefault(ShardSessionReadRequest.class,
                            new ShardSessionReadRequest(newIndex(6, 10)))
                .testAllPublicInstanceMethods(registry);
    }

    private ShardSessionRecord readSingleRecord(ShardIndex index) {
        Optional<ShardSessionRecord> record = registry.storage()
                                                      .read(index);
        assertThat(record).isPresent();
        return record.get();
    }

    private static NodeId newNode() {
        return NodeId.newBuilder()
                     .setValue(Identifier.newUuid())
                     .vBuild();
    }
}
