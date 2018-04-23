/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static com.google.common.base.Throwables.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("FutureObserver should")
class FutureObserverTest {

    @Test
    @DisplayName("instantiate self")
    void testCreateDefault() {
        final FutureObserver<String> observer = FutureObserver.create();
        final String value = "hello";
        observer.onNext(value);
        assertEquals(value, observer.toFuture().join());
        observer.onCompleted();
        assertEquals(value, observer.toFuture().join());
    }

    @Test
    @DisplayName("complete with null by default")
    void testDefaultNull() {
        final FutureObserver<String> observer = FutureObserver.create();
        observer.onCompleted();
        assertNull(observer.toFuture().join());
    }

    @Test
    @DisplayName("complete with given default value")
    void testDefaultValue() {
        final String defaultValue = "Aquaman";
        final FutureObserver<String> observer = FutureObserver.withDefault(defaultValue);
        observer.onCompleted();
        assertEquals(defaultValue, observer.toFuture().join());
    }

    @Test
    @DisplayName("fail to override value")
    void testFailToOverride() {
        final FutureObserver<String> observer = FutureObserver.create();
        observer.onNext("first");
        assertThrows(IllegalStateException.class, () -> observer.onNext("second"));
    }

    @Test
    @DisplayName("override value with error if onError() called")
    void testOverrideWithError() {
        final FutureObserver<String> observer = FutureObserver.create();
        final String value = "Titanic";
        observer.onNext(value);
        assertEquals(value, observer.toFuture().join());
        observer.onError(new IcebergCollisionException());
        final Throwable thrown = assertThrows(CompletionException.class,
                                              () -> observer.toFuture().join());
        final Throwable rootCause = getRootCause(thrown);
        assertThat(rootCause, instanceOf(IcebergCollisionException.class));
    }

    /**
     * An exception thrown on an event on colliding with an iceberg.
     *
     * <p>This exception type exists in order to uniquely flag an error origin.
     */
    private static final class IcebergCollisionException extends Exception {
        private static final long serialVersionUID = 0L;
    }
}
