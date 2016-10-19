/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.datastore.newapi;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.KeyFactory;

import java.util.Collection;
import java.util.LinkedList;

/**
 * @author Dmytro Dashenkov
 */
public class TestDatastoreWrapper extends DatastoreWrapper {

    private final Collection<String> kindsCache = new LinkedList<>();

    private TestDatastoreWrapper(Datastore datastore) {
        super(datastore);
    }

    public static TestDatastoreWrapper wrap(Datastore datastore) {
        return new TestDatastoreWrapper(datastore);
    }

    @Override
    public KeyFactory getKeyFactory(String kind) {
        kindsCache.add(kind);
        return super.getKeyFactory(kind);
    }

    public void dropAllTables() {
        for (String kind : kindsCache) {
            dropTable(kind);
        }

        kindsCache.clear();
    }
}
