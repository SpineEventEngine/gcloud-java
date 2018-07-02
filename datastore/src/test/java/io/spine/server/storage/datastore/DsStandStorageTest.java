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

import io.spine.server.entity.Entity;
import io.spine.server.stand.StandStorage;
import io.spine.server.stand.StandStorageTest;
import io.spine.server.storage.RecordStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("DsStandStorage should")
class DsStandStorageTest extends StandStorageTest {

    private static final TestDatastoreStorageFactory datastoreFactory =
            TestDatastoreStorageFactory.getDefaultInstance();

    @BeforeEach
    void setUp() {
        datastoreFactory.setUp();
    }

    @AfterEach
    void tearDown() {
        datastoreFactory.tearDown();
    }

    @Test
    @DisplayName("have RecordStorage instance")
    void testRecordStorage() {
        RecordStorage<?> recordStorage =
                ((DsStandStorage) newStorage(StandStorageRecord.class)).getRecordStorage();
        assertNotNull(recordStorage);
    }

    @Override
    protected StandStorage newStorage(Class<? extends Entity> cls) {
        return datastoreFactory.createStandStorage();
    }
}
