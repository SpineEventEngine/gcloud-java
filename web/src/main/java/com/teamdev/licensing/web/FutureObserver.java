/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web;

import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * An unary {@linkplain StreamObserver stream observer} which can provide
 * a {@link CompletableFuture} view on the stream.
 *
 * @author Dmytro Dashenkov
 */
public final class FutureObserver<T> implements StreamObserver<T> {

    private final CompletableFuture<T> future;
    @Nullable
    private final T defaultValue;

    private FutureObserver(@Nullable T defaultValue) {
        this.future = new CompletableFuture<>();
        this.defaultValue = defaultValue;
    }

    private FutureObserver() {
        this(null);
    }

    /**
     * Creates a new instance of {@code FutureObserver} with the given {@code defaultValue}.
     *
     * <p>The default value is used only if the {@link #onNext(Object)} method is never invoked and
     * the stream is {@linkplain #onCompleted() completed}. In such case, the future view is
     * completed with the given value.
     *
     * @param defaultValue the default value to complete the future with
     * @param <T> the type of the observed stream
     * @return new {@code FutureObserver}
     */
    public static <T> FutureObserver<T> withDefault(T defaultValue) {
        checkNotNull(defaultValue);
        return new FutureObserver<>(defaultValue);
    }

    /**
     * Creates a new instance of {@code FutureObserver}.
     *
     * <p>In case if the stream is {@linkplain #onCompleted() completed} with no value, the future
     * view completed with {@code null}.
     *
     * @param <T> the type of the observed stream
     * @return new {@code FutureObserver}
     * @see #withDefault(Object)
     */
    public static <T> FutureObserver<T> create() {
        return new FutureObserver<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNext(T value) {
        if (future.isDone()) {
            throw newIllegalStateException("FutureObserver may only be used for UNARY calls.");
        } else {
            future.complete(value);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(Throwable t) {
        future.obtrudeException(t);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onCompleted() {
        if (!future.isDone()) {
            future.complete(defaultValue);
        }
    }

    /**
     * Retrieves the future view on this observer result.
     *
     * <p>The future is completed when:
     * <ol>
     *     <li>the element of the unary stream arrives via {@link #onNext(Object)};
     *     <li>the stream is {@linkplain #onCompleted() completed} with no data;
     *     <li>the stream is {@linkplain #onError(Throwable) completed with an error}; in this case,
     *         the future is {@linkplain CompletableFuture#obtrudeException(Throwable) obtruded}
     *         with the error from the stream.
     * </ol>
     *
     * @return the future view on this observer
     */
    public CompletableFuture<T> toFuture() {
        return future;
    }
}
