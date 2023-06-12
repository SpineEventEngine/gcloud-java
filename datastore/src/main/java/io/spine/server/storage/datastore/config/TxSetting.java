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

package io.spine.server.storage.datastore.config;

import io.spine.annotation.Internal;
import io.spine.server.storage.datastore.DatastoreStorageFactory;

/**
 * Defines whether the storage created by the {@link DatastoreStorageFactory} uses Datastore
 * transactions for reads and writes.
 *
 * <p>This type is internal. Framework users may turn transactions on for a particular storage,
 * by calling {@link io.spine.server.storage.datastore.DatastoreStorageFactory.Builder#enableTransactions(Class)
 * DatastoreStorageFactory.newBuilder().enableTransactions(recordType)}.
 */
@Internal
public final class TxSetting {

    private final boolean txEnabled;

    /**
     * Creates a new instance of this setting, with the passed value as a feature flag for enabling
     * or disabling transactions for a particular storage.
     */
    private TxSetting(boolean txEnabled) {
        this.txEnabled = txEnabled;
    }

    /**
     * Creates a setting with disabled transactions.
     */
    public static TxSetting disabled() {
        return new TxSetting(false);
    }

    /**
     * Creates a setting with enabled transactions.
     */
    public static TxSetting enabled() {
        return new TxSetting(true);
    }

    /**
     * Tells whether the transactions should be enabled.
     */
    public boolean txEnabled() {
        return txEnabled;
    }
}
