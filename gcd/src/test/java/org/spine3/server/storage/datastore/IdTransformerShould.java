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

import org.junit.Test;
import org.spine3.test.storage.ProjectId;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.spine3.test.Tests.assertHasPrivateParameterlessCtor;

/**
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class IdTransformerShould {

    @Test
    public void have_private_constructor() {
        assertHasPrivateParameterlessCtor(IdTransformer.class);
    }

    @Test
    public void transform_numeric_ids_back_and_forth() {
        final Integer id = 42;
        final String stringRepr = IdTransformer.idToString(id);
        final Long restoredLong = IdTransformer.idFromString(stringRepr, IntImpl.class);
        assertNotNull(restoredLong);
        final Integer restored = restoredLong.intValue();
        assertEquals(id, restored);
    }

    @SuppressWarnings("StringEquality")
    @Test
    public void transform_string_ids_back_and_forth() {
        final String id = "foo";
        final String stringRepr = IdTransformer.idToString(id);
        final String restored = IdTransformer.idFromString(stringRepr, StringImpl.class);
        assertTrue(id == stringRepr);
        assertTrue(restored == stringRepr);
    }

    @Test
    public void transform_message_ids_back_and_forth() {
        final ProjectId genericId = ProjectId.newBuilder()
                                             .setId("foo-id")
                                             .build();
        final String stringRepr = IdTransformer.idToString(genericId);
        final ProjectId restored = IdTransformer.idFromString(stringRepr, MessageImpl.class);
        assertEquals(genericId, restored);
    }

    @Test
    public void transform_default_message_ids_back_and_forth() {
        final ProjectId genericId = ProjectId.getDefaultInstance();
        final String stringRepr = IdTransformer.idToString(genericId);
        final ProjectId restored = IdTransformer.idFromString(stringRepr, MessageImpl.class);
        assertEquals(genericId, restored);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fail_to_transform_custom_ids_to_string() {
        final CustomId customId = new CustomId();
        IdTransformer.idToString(customId);
    }

    private interface BaseParametrizedType<T> {
    }

    private static class IntImpl implements BaseParametrizedType<Integer> {
    }

    private static class StringImpl implements BaseParametrizedType<String> {
    }

    private static class MessageImpl implements BaseParametrizedType<ProjectId> {
    }

    private static class CustomId {
    }
}
