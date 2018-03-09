/*
 * Copyright 2018, TeamDev Ltd. All rights reserved.
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

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import io.spine.server.event.EventStore;
import io.spine.server.event.EventStoreShould;

import static io.spine.server.storage.datastore.TestDatastoreStorageFactory.getDefaultInstance;

/**
 * @author Dmytro Dashenkov
 */
public class DsBasedEventStoreShould extends EventStoreShould {

    private static final TestDatastoreStorageFactory datastoreFactory = getDefaultInstance();

    @After
    public void tearDown() throws Exception {
        datastoreFactory.tearDown();
    }

    @Override
    protected EventStore creteStore() {
        return EventStore.newBuilder()
                         .setStreamExecutor(MoreExecutors.directExecutor())
                         .setStorageFactory(datastoreFactory)
                         .withDefaultLogger()
                         .build();
    }
}
