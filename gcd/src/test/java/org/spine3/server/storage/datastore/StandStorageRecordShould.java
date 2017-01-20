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

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertTrue;

/**
 * @author Alex Tymchenko
 */
public class StandStorageRecordShould {

    @Test
    public void have_protected_constructor_for_extension() {
        final DatastoreRecordId someRecordId = DatastoreIdentifiers.of(Identifiers.newUuid());
        final boolean matches = hasProtectedConstructorWithSingleParam(
                StandStorageRecord.class, someRecordId);
        assertTrue(matches);
    }

    /**
     * Checks that the given {@code clazz} has the protected constructor with the
     * single parameter of the specified type.
     *
     * <p>This method invokes this constructor passing the given {@code constructorArg}
     * to include it into the coverage report.
     *
     * @param clazz          the class to check
     * @param constructorArg the constructor argument of the expected type
     * @param <I>            the expected type of the constructor argument
     * @return {@code true} if the protected constructor is present, {@code false} otherwise
     */
    public static <I> boolean hasProtectedConstructorWithSingleParam(
            Class<?> clazz, I constructorArg) {
        final Constructor constructor;
        try {
            constructor = clazz.getDeclaredConstructor(constructorArg.getClass());
        } catch (NoSuchMethodException ignored) {
            return false;
        }

        if (!Modifier.isProtected(constructor.getModifiers())) {
            return false;
        }

        final Class[] parameterTypes = constructor.getParameterTypes();
        if (parameterTypes.length != 1) {
            return false;
        }

        constructor.setAccessible(true);
        try {
            constructor.newInstance(constructorArg);
        } catch (Exception ignored) {
            return true;
        }
        return true;
    }
}
