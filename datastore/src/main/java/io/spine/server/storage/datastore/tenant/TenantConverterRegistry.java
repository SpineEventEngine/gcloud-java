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

package io.spine.server.storage.datastore.tenant;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Converter;
import io.spine.annotation.Internal;
import io.spine.core.TenantId;
import io.spine.server.storage.datastore.ProjectId;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.synchronizedMap;
import static java.util.Optional.ofNullable;

/**
 * A registry of the {@link NamespaceConverter}s.
 *
 * <p>The converters are mapped to the {@link ProjectId}s one-to-one, i.e. one GAE project may
 * have only one strategy of converting the {@link TenantId tenant IDs}.
 */
@Internal
public final class TenantConverterRegistry {

    private static final Map<ProjectId, NamespaceConverter> tenantIdConverters =
            synchronizedMap(newHashMap());

    /**
     * Prevents the utility class instantiation.
     */
    private TenantConverterRegistry() {
    }

    /**
     * Registers a {@link Converter Converter} from string datastore
     * namespace into {@link TenantId TenantId} for the given {@link ProjectId}.
     *
     * <p>After this converter has been registered, all the Datastore namespace operations will use
     * it instead of the {@linkplain Namespace default behavior}.
     *
     * <p>Note, that this method should be called only once per one instance of {@link ProjectId}.
     * All the subsequent invocations will cause {@code IllegalStateException}s.
     *
     * @param converter
     *         the converter to use for the
     *         namespace-to-{@link TenantId TenantId} and vice versa
     *         conversions
     * @see Namespace
     */
    @Internal
    public static void registerNamespaceConverter(ProjectId projectId,
                                                  NamespaceConverter converter) {
        checkNotNull(projectId);
        checkNotNull(converter);
        NamespaceConverter pastConverter = tenantIdConverters.put(projectId,
                                                                  converter);
        checkState(pastConverter == null,
                   "A namespace converter has already been registered.");
    }

    /**
     * Retrieves the registered {@link NamespaceConverter}.
     *
     * @return the {@linkplain #registerNamespaceConverter registered}
     *         {@link NamespaceConverter} wrapped into {@link Optional} or
     *         {@link Optional#empty() Optional.empty()} if the converter has never been registered
     */
    @VisibleForTesting
    public static Optional<NamespaceConverter> getNamespaceConverter(ProjectId projectId) {
        checkNotNull(projectId);
        NamespaceConverter converter = tenantIdConverters.get(projectId);
        return ofNullable(converter);
    }
}
