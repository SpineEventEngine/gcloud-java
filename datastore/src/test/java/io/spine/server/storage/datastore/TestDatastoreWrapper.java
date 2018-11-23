/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.KeyFactory;
import io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Custom extension of the {@link DatastoreWrapper} for the integration testing.
 *
 * @see TestDatastoreStorageFactory
 */
public class TestDatastoreWrapper extends DatastoreWrapper {

    private static final Collection<String> kindsCache = new ArrayList<>();

    private TestDatastoreWrapper(Datastore datastore) {
        super(datastore, TestNamespaceSuppliers.singleTenant());
    }

    public static TestDatastoreWrapper wrap(Datastore datastore) {
        return new TestDatastoreWrapper(datastore);
    }

    @Override
    public KeyFactory getKeyFactory(Kind kind) {
        kindsCache.add(kind.getValue());
        return super.getKeyFactory(kind);
    }

    /**
     * Deletes all records from the datastore.
     */
    public void dropAllTables() {
        log().debug("Dropping all tables");
        for (String kind : kindsCache) {
            dropTable(kind);
        }

        kindsCache.clear();
    }
}
