/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore.tenant;

import com.google.common.base.Converter;
import com.google.errorprone.annotations.Immutable;
import io.spine.annotation.SPI;
import io.spine.core.TenantId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link Converter} for {@link TenantId} and the string Datastore namespace.
 *
 * <p>Define this converter in case if there is a need to override
 * the {@linkplain Namespace default conversion behavior}.
 *
 * <p>The implementation of the conversion must be commutative, meaning that the restored
 * {@link TenantId} values should be {@linkplain TenantId#equals(Object) equal} to the initial.
 *
 * @see Namespace
 */
@SPI
@Immutable
public abstract class NamespaceConverter extends Converter<String, TenantId> {

    /**
     * The value returned by a converted when it encounters a Datastore namespace which does not
     * represent a tenant ID.
     */
    public static final TenantId NOT_A_TENANT = TenantId.getDefaultInstance();

    @Override
    protected final TenantId doForward(String s) {
        checkNotNull(s);
        return toTenantId(s);
    }

    @Override
    protected final String doBackward(TenantId tenantId) {
        checkNotNull(tenantId);
        return toString(tenantId);
    }

    /**
     * Converts the given {@link TenantId} into a string representing a Datastore namespace.
     */
    protected abstract String toString(TenantId tenantId);

    /**
     * Converts the given string representing a Datastore namespace into a {@link TenantId}.
     *
     * <p>If the given namespace does not represent a tenant, as defined by the internal rules of
     * this converter, returns {@link #NOT_A_TENANT}.
     */
    protected abstract TenantId toTenantId(String namespace);
}
