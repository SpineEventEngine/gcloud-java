/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import io.spine.base.Identifier;
import io.spine.server.NodeId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.storage.datastore.given.TestShardIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@DisplayName("DsShardedWorkRegistry should")
public class DsShardedWorkRegistryTest {

    private final TestDatastoreStorageFactory factory =
            TestDatastoreStorageFactory.defaultInstance();

    @BeforeEach
    void setUp() {
        factory.setUp();
    }

    @AfterEach
    void tearDown() {
        factory.tearDown();
    }

    @Test
    @DisplayName("pick up the session and write a corresponding record to the storage")
    public void pickUp() {
        DsShardedWorkRegistry registry = new DsShardedWorkRegistry(factory);

        ShardIndex index = TestShardIndex.newIndex(1, 15);
        NodeId nodeId = NodeId.newBuilder()
                              .setValue(Identifier.newUuid())
                              .vBuild();
        Optional<ShardProcessingSession> session = registry.pickUp(index, nodeId);
        assertTrue(session.isPresent());
        assertEquals(index, session.get().shardIndex());
    }
}
