/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.Value;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Function;

/**
 * A storage policy of an entity {@linkplain io.spine.server.entity.storage.Column column}.
 *
 * <p>An {@link #apply(Object)} method expects non-{@code null} values
 *
 * @apiNote the type of param {@code T} does not match the param of {@code Value} because there can
 *        be column types which are converted to some other data type before being converted to
 *        {@code Value}, i.e. a {@link com.google.protobuf.Message Message} is converted to a
 *        {@code String} and thus is stored as {@code Value<String>} and not {@code Value<Message>}
 *        (which furthermore doesn't exist).
 */
public interface PersistenceStrategy<T> extends Function<T, Value<?>> {

    /**
     * A convenience shortcut.
     *
     * <p>...
     */
    @SuppressWarnings("unchecked") // It's up to caller to provide a valid object.
    default Value<?> applyTo(@Nullable Object object) {
        if (object == null) {
            return NullValue.of();
        }
        T value = (T) object;
        Value<?> result = apply(value);
        return result;
    }
}
