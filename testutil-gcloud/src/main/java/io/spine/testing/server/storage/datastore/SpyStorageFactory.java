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

package io.spine.testing.server.storage.datastore;

import io.spine.server.storage.datastore.DatastoreWrapper;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link TestDatastoreStorageFactory} which allows to inject a custom {@link DatastoreWrapper}.
 */
public final class SpyStorageFactory extends TestDatastoreStorageFactory {

    private static DatastoreWrapper injectedWrapper = null;

    /**
     * Injects a given {@code DatastoreWrapper} into the storage factory.
     *
     * <p>All storages created by this factory will operate based on the given wrapper.
     *
     * <p>Should be called before the {@code SpyStorageFactory} instance is created.
     */
    public static void injectWrapper(DatastoreWrapper wrapper) {
        injectedWrapper = wrapper;
    }

    public SpyStorageFactory() {
        super(checkNotNull(injectedWrapper).datastore());
    }

    @Override
    protected DatastoreWrapper createDatastoreWrapper(boolean multitenant) {
        return injectedWrapper;
    }
}
