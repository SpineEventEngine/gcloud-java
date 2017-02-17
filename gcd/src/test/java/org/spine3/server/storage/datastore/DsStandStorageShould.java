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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.spine3.server.stand.StandStorage;
import org.spine3.server.stand.StandStorageShould;
import org.spine3.server.storage.RecordStorage;

import static org.junit.Assert.assertNotNull;

/**
 * @author Dmytro Dashenkov
 */
public class DsStandStorageShould extends StandStorageShould {

    private static final TestDatastoreStorageFactory datastoreFactory
            = TestDatastoreStorageFactory.getDefaultInstance();

    @Before
    public void setUp() throws Exception {
        datastoreFactory.setUp();
    }

    @After
    public void tearDown() throws Exception {
        datastoreFactory.tearDown();
    }

    @Test
    public void contain_record_storage() {
        final RecordStorage<?> recordStorage = ((DsStandStorage) getStorage()).getRecordStorage();
        assertNotNull(recordStorage);
    }

    @Override
    protected StandStorage getStorage() {
        return datastoreFactory.createStandStorage();
    }
}
