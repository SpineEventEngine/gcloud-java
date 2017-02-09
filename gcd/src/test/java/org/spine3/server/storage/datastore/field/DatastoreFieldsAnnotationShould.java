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

package org.spine3.server.storage.datastore.field;

import org.junit.Test;

import java.lang.annotation.Annotation;

import static org.junit.Assert.assertNull;

/**
 * @author Dmytro Dashenkov.
 */
public class DatastoreFieldsAnnotationShould {

    @Test
    public void not_be_visible_in_runtime() {
        final Class<MockFields> fieldsClass = MockFields.class;
        @SuppressWarnings("ReflectionForUnavailableAnnotation") // That's what we test here
        final Annotation annotation = fieldsClass.getAnnotation(DatastoreFields.class);
        assertNull(annotation);
    }

    @SuppressWarnings("EmptyClass") // Required as a bearer for the {@code @DatastoreFields} annotation.
    @DatastoreFields
    private static class MockFields {

    }
}
