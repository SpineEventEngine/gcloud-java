/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.datastore.record;

import com.google.cloud.datastore.Key;
import com.google.common.testing.NullPointerTester;
import io.spine.server.storage.datastore.Kind;
import io.spine.test.storage.StgProject;
import io.spine.testing.UtilityClassTest;
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("`Entities` should")
final class EntitiesTest extends UtilityClassTest<Entities> {

    EntitiesTest() {
        super(Entities.class);
    }

    @Override
    protected void configure(NullPointerTester tester) {
        super.configure(tester);
        tester.setDefault(Key.class, someKey());
    }

    private static Key someKey() {
        var kind = Kind.of(EntitiesTest.class.getName());
        return TestDatastoreStorageFactory
                .local()
                .newDatastoreWrapper(false)
                .keyFactory(kind)
                .newKey(42);
    }

    @Test
    @DisplayName("retrieve default message instance for `null` entity")
    void testNull() {
        var typeUrl = TypeUrl.from(StgProject.getDescriptor());
        var expected = StgProject.getDefaultInstance();
        StgProject actual = Entities.toMessage(null, typeUrl);

        assertEquals(expected, actual);
    }
}
