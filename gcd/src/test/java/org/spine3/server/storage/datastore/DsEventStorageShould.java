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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.spine3.server.event.EventStorage;
import org.spine3.server.event.EventStorageShould;

import static org.junit.Assert.assertNotNull;

@SuppressWarnings("InstanceMethodNamingConvention")
public class DsEventStorageShould extends EventStorageShould {

    private static final TestDatastoreStorageFactory datastoreFactory = TestDatastoreStorageFactory.getDefaultInstance();

    @Override
    public void tearDownEventStorageTest() {
        super.tearDownEventStorageTest();
        datastoreFactory.clear();
    }

    @Override
    protected EventStorage getStorage() {
        return datastoreFactory.createEventStorage();
    }

    @BeforeClass
    public static void setUpClass() {
        datastoreFactory.setUp();
    }

    @After
    public void tearDownTest() {
        datastoreFactory.tearDown();
    }

    @AfterClass
    public static void tearDownClass() {
        datastoreFactory.tearDown();
    }

    @Test
    public void provide_access_to_DatastoreWrapper_for_extensibility() {
        final DsEventStorage storage = (DsEventStorage) getStorage();
        final DatastoreWrapper datastore = storage.getDatastore();
        assertNotNull(datastore);
    }

    // TODO:2017-03-14:dmytro.dashenkov: Fix overprecise tests in core-java and reenable.
    @Ignore
    @Test
    @Override
    public void find_events_which_happened_after_a_point_in_time_CASE_secs_EQUAL_and_nanos_BIGGER() {
        super.find_events_which_happened_after_a_point_in_time_CASE_secs_EQUAL_and_nanos_BIGGER();
    }

    @Ignore
    @Test
    @Override
    public void find_events_which_happened_before_a_point_in_time_CASE_secs_EQUAL_and_nanos_LESS() {
        super.find_events_which_happened_before_a_point_in_time_CASE_secs_EQUAL_and_nanos_LESS();
    }
}
