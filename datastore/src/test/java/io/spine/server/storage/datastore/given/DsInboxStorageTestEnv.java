/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore.given;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxMessageStatus;
import io.spine.server.delivery.ShardIndex;
import io.spine.test.delivery.Calc;
import io.spine.testing.server.TestEventFactory;
import io.spine.type.TypeUrl;

import java.security.SecureRandom;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.base.Time.currentTime;
import static io.spine.server.delivery.given.TestInboxMessages.toDeliver;
import static io.spine.server.storage.datastore.given.TestShardIndex.newIndex;

/**
 * Test environment for {@link DsInboxStorageTest}.
 */
public class DsInboxStorageTestEnv {

    private static final TestEventFactory factory =
            TestEventFactory.newInstance(DsInboxStorageTestEnv.class);

    private static final SecureRandom random = new SecureRandom();

    /**
     * Prevents this utility class from an instantiation.
     */
    private DsInboxStorageTestEnv() {
    }

    /**
     * Generates an {@link InboxMessage} with the specified values.
     *
     * The message values are set as if it was received at {@code whenReceived} time
     * and its status was {@link InboxMessageStatus#TO_DELIVER TO_DELIVER}.
     */
    public static InboxMessage generate(int shardIndex, int totalShards, Timestamp whenReceived) {
        checkNotNull(whenReceived);
        InboxMessage message = toDeliver("target-entity-id", TypeUrl.of(Calc.class), whenReceived);

        InboxMessageId modifiedId =
                message.getId()
                       .toBuilder()
                       .setIndex(newIndex(shardIndex, totalShards))
                       .vBuild();
        InboxMessage result =
                message.toBuilder()
                       .setId(modifiedId)
                       .vBuild();
        return result;
    }

    /**
     * Generates {@code totalMessage} in a selected shard.
     *
     * <p>Each message is generated as received {@code now} and in
     * {@link InboxMessageStatus#TO_DELIVER TO_DELIVER} status.
     */
    public static ImmutableList<InboxMessage> generate(int totalMessages, ShardIndex index) {
        ImmutableList.Builder<InboxMessage> builder = ImmutableList.builder();
        for (int msgCounter = 0; msgCounter < totalMessages; msgCounter++) {

            InboxMessage msg = generate(index.getIndex(), index.getOfTotal(), currentTime());
            builder.add(msg);
        }
        return builder.build();
    }
}
