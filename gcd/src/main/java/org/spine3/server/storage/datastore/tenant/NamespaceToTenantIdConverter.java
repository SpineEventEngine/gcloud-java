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

package org.spine3.server.storage.datastore.tenant;

import com.google.common.base.Converter;
import org.spine3.annotations.SPI;
import org.spine3.users.TenantId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link Converter} for {@link TenantId} and the string Datastore namespace.
 *
 * <p>Define this converter in case if there is a need to override
 * the {@linkplain Namespace default convertion behavior}.
 *
 * <p>The implementation of the convertion must be commutative, meaning that the restored
 * {@link TenantId} values should be {@linkplain TenantId#equals(Object) equal} to the initial.
 *
 * @author Dmytro Dashenkov
 * @see Namespace
 */
@SPI
public abstract class NamespaceToTenantIdConverter extends Converter<String, TenantId> {

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
     */
    protected abstract TenantId toTenantId(String namespace);
}
