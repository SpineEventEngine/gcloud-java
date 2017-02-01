/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.protobuf.Any;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class DatastoreWrapperShould {

    @Test
    public void work_with_transactions_if_necessary() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.commitTransaction();
        assertFalse(wrapper.isTransactionActive());
    }

    @Test
    public void rollback_transactions() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.rollbackTransaction();
        assertFalse(wrapper.isTransactionActive());
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_start_transaction_if_one_is_active() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore());
        try {
            wrapper.startTransaction();
            assertTrue(wrapper.isTransactionActive());
            wrapper.startTransaction();
        } finally {
            wrapper.rollbackTransaction();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_finish_not_active_transaction() {
        final DatastoreWrapper wrapper = DatastoreWrapper.wrap(Given.testDatastore());
        wrapper.startTransaction();
        assertTrue(wrapper.isTransactionActive());
        wrapper.commitTransaction();
        assertFalse(wrapper.isTransactionActive());
        wrapper.rollbackTransaction();
    }

    // Some variables act in different cases and should have self-explainatory names
    @SuppressWarnings("UnnecessaryLocalVariable")
    @Test
    public void support_big_bulk_reads() throws InterruptedException {
        final int bulkSize = 1001;
        final int maxWritesPerCall = 500;
        final int firstPartStart = 0;
        final int firstPartEnd = firstPartStart + maxWritesPerCall;
        final int secondPartStart = firstPartEnd;
        final int secondPartEnd = secondPartStart + maxWritesPerCall;
        final int thirdPartStart = secondPartEnd;
        final int thirdPartEnd = bulkSize;


        final TestDatastoreWrapper wrapper = TestDatastoreWrapper.wrap(Given.testDatastore(), false);
        final Map<Key, Entity> entities = Given.nEntities(bulkSize, wrapper);
        final List<Entity> sourceEntities = new ArrayList<>(entities.values());

        final Entity[] firstPart = new Entity[maxWritesPerCall];
        final Entity[] secondPart = new Entity[maxWritesPerCall];
        final Entity[] thirdPart = new Entity[1]; // Only 1 element left. Change this when changing bulkSize
        sourceEntities.subList(firstPartStart, firstPartEnd).toArray(firstPart);
        sourceEntities.subList(secondPartStart, secondPartEnd).toArray(secondPart);
        sourceEntities.subList(thirdPartStart, thirdPartEnd).toArray(thirdPart);

        wrapper.createOrUpdate(firstPart);
        wrapper.createOrUpdate(secondPart);
        wrapper.createOrUpdate(thirdPart);

        // Wait some time to make sure the writing is complete
        try {
            Thread.sleep(bulkSize * 5);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }

        final Collection<Entity> readEntities = wrapper.read(entities.keySet());
        assertEquals(sourceEntities.size(), readEntities.size());
        assertTrue(sourceEntities.containsAll(readEntities));

        wrapper.dropAllTables();
    }

    private static class Given {

        private static final String GENERIC_ENTITY_KIND = "my.entity";

        private static Datastore testDatastore() {
            final boolean onCi = "true".equals(System.getenv("CI"));
            return onCi
                    ? TestDatastoreStorageFactory.TestingDatastoreSingleton.INSTANCE.value
                    : TestDatastoreStorageFactory.DefaultDatastoreSingleton.INSTANCE.value;
        }

        private static Map<Key, Entity> nEntities(int n, DatastoreWrapper wrapper) {

            final Map<Key, Entity> result = new HashMap<>(n);
            for (int i = 0; i < n; i++) {
                final Any message = Any.getDefaultInstance();
                final DatastoreRecordId recordId = new DatastoreRecordId(String.format("record-%s", i));
                final Key key = DatastoreIdentifiers.keyFor(wrapper, GENERIC_ENTITY_KIND, recordId);
                final Entity entity = Entities.messageToEntity(message, key);
                result.put(key, entity);
            }
            return result;
        }
    }
}
