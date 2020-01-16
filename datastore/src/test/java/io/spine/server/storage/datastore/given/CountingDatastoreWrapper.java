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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StructuredQuery;
import io.spine.server.storage.datastore.DsQueryIterator;
import io.spine.testing.server.storage.datastore.TestDatastoreWrapper;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Iterator;

/**
 * A {@link TestDatastoreWrapper} which counts the number of calls for certain types of Datastore
 * operations.
 */
public final class CountingDatastoreWrapper extends TestDatastoreWrapper {

    private int readByKeysCount;
    private int readByQueryCount;
    private int deleteCount;

    public CountingDatastoreWrapper(Datastore datastore, boolean waitForConsistency) {
        super(datastore, waitForConsistency);
    }

    @Override
    public Entity read(Key key) {
        Entity result = super.read(key);
        readByKeysCount++;
        return result;
    }

    @Override
    public Iterator<@Nullable Entity> read(Iterable<Key> keys) {
        Iterator<@Nullable Entity> result = super.read(keys);
        readByKeysCount++;
        return result;
    }

    @Override
    public <R> DsQueryIterator<R> read(StructuredQuery<R> query) {
        DsQueryIterator<R> result = super.read(query);
        readByQueryCount++;
        return result;
    }

    @Override
    protected void deleteEntities(Collection<Entity> entities) {
        super.deleteEntities(entities);
        deleteCount++;
    }

    public int readByKeysCount() {
        return readByKeysCount;
    }

    public int readByQueryCount() {
        return readByQueryCount;
    }

    public int deleteCount() {
        return deleteCount;
    }
}
