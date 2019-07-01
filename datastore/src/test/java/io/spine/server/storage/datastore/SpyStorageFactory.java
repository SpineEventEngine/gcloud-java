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

import io.spine.annotation.Internal;
import io.spine.server.ContextSpec;

import static org.mockito.Mockito.spy;

/**
 * A {@link TestDatastoreStorageFactory} which spies on its {@link DatastoreWrapper}.
 *
 * This class is not moved to the
 * {@linkplain io.spine.server.storage.datastore.given.DsRecordStorageTestEnv test environment}
 * because it uses the package-private method of {@link DatastoreWrapper}.
 */
final class SpyStorageFactory extends TestDatastoreStorageFactory {

    private static DatastoreWrapper spyWrapper = null;

    static void injectWrapper(DatastoreWrapper wrapper) {
        spyWrapper = spy(wrapper);
    }

    SpyStorageFactory() {
        super(spyWrapper.datastore());
    }

    @Internal
    @Override
    protected DatastoreWrapper createDatastoreWrapper(ContextSpec spec) {
        return spyWrapper;
    }
}
