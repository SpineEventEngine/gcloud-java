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

package io.spine.server.storage.datastore.type;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;
import io.spine.server.entity.storage.ColumnTypeRegistry;

import static io.spine.server.storage.datastore.type.DsColumnTypes.booleanType;
import static io.spine.server.storage.datastore.type.DsColumnTypes.doubleType;
import static io.spine.server.storage.datastore.type.DsColumnTypes.floatType;
import static io.spine.server.storage.datastore.type.DsColumnTypes.integerType;
import static io.spine.server.storage.datastore.type.DsColumnTypes.longType;
import static io.spine.server.storage.datastore.type.DsColumnTypes.messageType;
import static io.spine.server.storage.datastore.type.DsColumnTypes.stringType;
import static io.spine.server.storage.datastore.type.DsColumnTypes.timestampType;
import static io.spine.server.storage.datastore.type.DsColumnTypes.versionType;

/**
 * A factory of the Datastore-specific {@link ColumnTypeRegistry ColumnTypeRegistries}.
 *
 * @author Dmytro Dashenkov
 */
public final class DatastoreTypeRegistryFactory {

    private static final ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> DEFAULT_REGISTRY =
            ColumnTypeRegistry.<DatastoreColumnType<?, ?>>newBuilder()
                              .put(String.class, stringType())
                              .put(Integer.class, integerType())
                              .put(Long.class, longType())
                              .put(Double.class, doubleType())
                              .put(Float.class, floatType())
                              .put(Boolean.class, booleanType())
                              .put(Timestamp.class, timestampType())
                              .put(Version.class, versionType())
                              .put(AbstractMessage.class, messageType())
                              .build();

    private DatastoreTypeRegistryFactory() {
        // Prevent initialization of a utility class
    }

    /**
     * Retrieves a default
     * {@link ColumnTypeRegistry ColumnTypeRegistry&lt;? extends DatastoreColumnType&gt;}.
     *
     * The returned registry contains the
     * {@linkplain io.spine.server.entity.storage.ColumnType column types} declarations for:
     * <ul>
     * <li> {@code String}
     * <li> {@code Integer}
     * <li> {@code Long}
     * <li> {@code Boolean}
     * <li> {@link Timestamp} stored as {@link com.google.cloud.Timestamp
     * com.google.cloud.Timestamp}
     * <li> {@link AbstractMessage Message} stored as a {@code String} retrieved form a
     * {@link io.spine.string.Stringifier Stringifier}
     * <li> {@link Version} stored as an {@code int} version number
     * </ul>
     *
     * @return the default {@code ColumnTypeRegistry} for storing the Entity Columns in Datastore
     */
    public static ColumnTypeRegistry<? extends DatastoreColumnType<?, ?>> defaultInstance() {
        return DEFAULT_REGISTRY;
    }

    /**
     * Retrieves a builder with all the {@linkplain #defaultInstance() predefined values} set.
     */
    public static ColumnTypeRegistry.Builder<? extends DatastoreColumnType<?, ?>> predefinedValuesAnd() {
        return ColumnTypeRegistry.newBuilder(DEFAULT_REGISTRY);
    }
}
