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

import com.google.cloud.datastore.Entity;
import com.google.protobuf.Message;
import io.spine.test.storage.Project;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.datastore.Entities.entitiesToMessages;
import static io.spine.server.storage.datastore.given.TestCases.HAVE_PRIVATE_UTILITY_CTOR;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static java.util.Collections.emptyIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("Entities should")
class EntitiesTest {

    @Test
    @DisplayName(HAVE_PRIVATE_UTILITY_CTOR)
    void testPrivateCtor() {
        assertHasPrivateParameterlessCtor(Entities.class);
    }

    @Test
    @DisplayName("retrieve default message instance for null entity")
    void testNull() {
        TypeUrl typeUrl = TypeUrl.from(Project.getDescriptor());
        Project expected = Project.getDefaultInstance();
        Project actual = Entities.entityToMessage(null, typeUrl);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("retrieve default message instance for null entity in collection")
    void testNullInCollection() {
        TypeUrl typeUrl = TypeUrl.from(Project.getDescriptor());
        List<Entity> listOfNulls = newArrayList(null, null, null, null);
        Iterator<Message> iterOfDefaults = entitiesToMessages(listOfNulls.iterator(), typeUrl);
        List<Message> listOfDefaults = newArrayList(iterOfDefaults);

        assertThat(listOfDefaults).hasSize(listOfNulls.size());
        Project expectedValue = Project.getDefaultInstance();
        for (Message message : listOfDefaults) {
            assertEquals(expectedValue, message);
        }
    }

    @Test
    @DisplayName("retrieve empty collection on empty list")
    void testEmptyCollection() {
        TypeUrl typeUrl = TypeUrl.from(Project.getDescriptor());
        Iterator<Message> converted = entitiesToMessages(emptyIterator(), typeUrl);
        assertNotNull(converted);
        assertFalse(converted.hasNext());
    }
}
