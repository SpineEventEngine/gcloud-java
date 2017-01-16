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

import com.google.cloud.datastore.Entity;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.junit.Test;
import org.spine3.protobuf.TypeUrl;
import org.spine3.test.Tests;
import org.spine3.test.storage.Project;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.spine3.test.Verify.assertSize;

/**
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class EntitiesShould {

    @Test
    public void have_private_constructor() {
        assertTrue(Tests.hasPrivateUtilityConstructor(Entities.class));
    }

    @Test
    public void retrieve_default_message_instance_for_null_entity() {
        final TypeUrl typeUrl = TypeUrl.from(Project.getDescriptor());
        final Project expected = Project.getDefaultInstance();
        final Project actual = Entities.entityToMessage(null, typeUrl);

        assertEquals(expected, actual);
    }

    @Test
    public void retrieve_default_message_for_each_null_entity_in_collection() {
        final TypeUrl typeUrl = TypeUrl.from(Project.getDescriptor());
        final List<Entity> listOfNulls = Lists.newArrayList(null, null, null, null);
        final Collection<Message> listOfDefaults = Entities.entitiesToMessages(listOfNulls, typeUrl);
        assertSize(listOfNulls.size(), listOfDefaults);
        final Project expectedValue = Project.getDefaultInstance();
        for (Message message : listOfDefaults) {
            assertEquals(expectedValue, message);
        }
    }

    @Test
    public void retrieve_empty_collection_on_empty_list() {
        final List<Entity> empty = Collections.emptyList();
        final TypeUrl typeUrl = TypeUrl.from(Project.getDescriptor());
        final Collection<Message> converted = Entities.entitiesToMessages(empty, typeUrl);
        assertNotNull(converted);
        assertTrue(converted.isEmpty());
    }
}
