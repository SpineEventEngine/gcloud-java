/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.testing.NullPointerTester;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.util.Timestamps;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxMessageStatus;
import io.spine.server.delivery.InboxReadRequest;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.delivery.InboxStorageTest;
import io.spine.server.delivery.Page;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.datastore.given.DsInboxStorageTestEnv;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Time.currentTime;
import static io.spine.server.delivery.InboxMessageStatus.DELIVERED;
import static io.spine.server.delivery.InboxMessageStatus.TO_DELIVER;
import static io.spine.server.storage.datastore.given.DsInboxStorageTestEnv.generate;
import static io.spine.server.storage.datastore.given.TestShardIndex.newIndex;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@DisplayName("`DsInboxStorage` should")
class DsInboxStorageTest extends InboxStorageTest {

    private final TestDatastoreStorageFactory factory = TestDatastoreStorageFactory.local();

    @Override
    @BeforeEach
    protected void setUp() {
        factory.setUp();
        super.setUp();
    }

    @Override
    @AfterEach
    protected void tearDown() {
        factory.tearDown();
        super.tearDown();
    }

    @Override
    protected InboxStorage storage() {
        return factory.createInboxStorage(false);
    }

    @Test
    @DisplayName("read and write an `InboxMessage` instance")
    void readAndWriteSingleMessage() {
        InboxStorage storage = storage();

        InboxMessage msg = DsInboxStorageTestEnv.generate(3, 24, currentTime());
        storage.write(msg);
        readAndCompare(storage, msg);

        InboxMessage anotherMsg = DsInboxStorageTestEnv.generate(42, 100, currentTime());
        storage.write(anotherMsg.getId(), anotherMsg);
        readAndCompare(storage, anotherMsg);
    }

    @Test
    @DisplayName("read and write multiple `InboxMessage` instances")
    void readAndWriteMultipleMessages() {
        InboxStorage storage = storage();
        int totalMessages = 10;
        ShardIndex index = newIndex(1, 18);

        checkEmpty(storage, index);

        ImmutableList<InboxMessage> messages = generate(totalMessages, index);

        storage.writeAll(messages);

        ImmutableList<InboxMessage> contents = readAllAndCompare(storage, index, messages);
        assertThat(contents)
                .isInStrictOrder(comparing(InboxMessage::getWhenReceived, Timestamps.comparator()));
    }

    @Test
    @DisplayName("remove selected `InboxMessage` instances")
    void removeMessages() {
        ShardIndex index = newIndex(6, 7);
        ImmutableList<InboxMessage> messages = generate(20, index);
        InboxStorage storage = storage();
        storage.writeAll(messages);

        readAllAndCompare(storage, index, messages);

        UnmodifiableIterator<InboxMessage> iterator = messages.iterator();
        InboxMessage first = iterator.next();
        InboxMessage second = iterator.next();

        storage.removeAll(ImmutableList.of(first, second));

        // Make a `List` from the rest of the elements. Those deleted aren't included.
        ImmutableList<InboxMessage> remainder = ImmutableList.copyOf(iterator);

        readAllAndCompare(storage, index, remainder);

        storage.removeAll(remainder);
        checkEmpty(storage, index);
    }

    @Test
    @DisplayName("do nothing if removing inexistent `InboxMessage` instances")
    void doNothingIfRemovingInexistentMessages() {

        InboxStorage storage = storage();
        ShardIndex index = newIndex(6, 7);
        checkEmpty(storage, index);

        ImmutableList<InboxMessage> messages = generate(40, index);
        storage.removeAll(messages);

        checkEmpty(storage, index);
    }

    @Test
    @DisplayName("mark messages delivered")
    void markMessagedDelivered() {
        ShardIndex index = newIndex(3, 71);
        ImmutableList<InboxMessage> messages = generate(10, index);
        InboxStorage storage = storage();
        storage.writeAll(messages);

        ImmutableList<InboxMessage> nonDelivered = readAllAndCompare(storage, index, messages);
        nonDelivered.iterator()
                    .forEachRemaining((m) -> assertEquals(TO_DELIVER, m.getStatus()));

        // Leave the first one in `TO_DELIVER` status and mark the rest as `DELIVERED`.
        UnmodifiableIterator<InboxMessage> iterator = messages.iterator();
        InboxMessage remainingNonDelivered = iterator.next();
        ImmutableList<InboxMessage> toMarkDelivered = ImmutableList.copyOf(iterator);
        List<InboxMessage> markedDelivered = markDelivered(toMarkDelivered);

        storage.writeAll(markedDelivered);
        ImmutableList<InboxMessage> originalMarkedDelivered =
                toMarkDelivered.stream()
                               .map(m -> m.toBuilder()
                                          .setStatus(InboxMessageStatus.DELIVERED)
                                          .vBuild())
                               .collect(toImmutableList());

        // Check that both `TO_DELIVER` message and those marked `DELIVERED` are stored as expected.
        ImmutableList<InboxMessage> readResult = storage.readAll(index, Integer.MAX_VALUE)
                                                        .contents();
        assertTrue(readResult.contains(remainingNonDelivered));
        assertTrue(readResult.containsAll(originalMarkedDelivered));
    }

    private static List<InboxMessage> markDelivered(ImmutableList<InboxMessage> toMarkDelivered) {
        return toMarkDelivered.stream()
                              .map(m -> m.toBuilder()
                                         .setStatus(DELIVERED)
                                         .vBuild())
                              .collect(toList());
    }

    @Test
    @DisplayName("not accept `null` values in public API methods")
    void notAcceptNulls() {
        new NullPointerTester()
                .setDefault(ShardIndex.class, newIndex(4, 5))
                .setDefault(InboxMessage.class, InboxMessage.getDefaultInstance())
                .setDefault(InboxMessageId.class, InboxMessageId.getDefaultInstance())
                .setDefault(InboxReadRequest.class,
                            new InboxReadRequest(InboxMessageId.getDefaultInstance()))
                .testAllPublicInstanceMethods(storage());
    }

    @CanIgnoreReturnValue
    private static ImmutableList<InboxMessage>
    readAllAndCompare(InboxStorage storage, ShardIndex idx, ImmutableList<InboxMessage> expected) {
        Page<InboxMessage> page = storage.readAll(idx, Integer.MAX_VALUE);
        assertEquals(expected.size(), page.size());

        ImmutableList<InboxMessage> contents = page.contents();
        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(contents));
        return contents;
    }

    private static void checkEmpty(InboxStorage storage, ShardIndex index) {
        Page<InboxMessage> emptyPage = storage.readAll(index, 10);
        assertEquals(0, emptyPage.size());
        assertTrue(emptyPage.contents()
                            .isEmpty());
        assertFalse(emptyPage.next()
                             .isPresent());
    }

    private static void readAndCompare(InboxStorage storage, InboxMessage msg) {
        Optional<InboxMessage> optional = storage.read(new InboxReadRequest(msg.getId()));
        assertTrue(optional.isPresent());

        InboxMessage readResult = optional.get();
        assertEquals(msg, readResult);
    }
}
