/*
 *
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
 *
 */
package org.spine3.server.storage.datastore;

import org.junit.Test;
import org.spine3.base.Identifiers;
import org.spine3.test.Tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Alex Tymchenko
 */
public class DatastoreIdentifiersShould {

    @Test
    public void have_private_constructor() {
        assertTrue(Tests.hasPrivateUtilityConstructor(DatastoreIdentifiers.class));
    }

    @Test(expected = IllegalStateException.class)
    public void not_accept_empty_String_as_identifier_source() {
        DatastoreIdentifiers.of("");
    }

    @Test
    public void wrap_non_empty_String_into_record_identifier() {
        final String idAsString = Identifiers.newUuid();
        final DatastoreRecordId recordId = DatastoreIdentifiers.of(idAsString);

        assertNotNull(recordId);
        assertEquals(idAsString, recordId.getValue());
    }
}
