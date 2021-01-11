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

import io.spine.annotation.Internal;

/**
 * An interface of the factories of {@link NamespaceConverter}s.
 */
@FunctionalInterface
@Internal
public interface NsConverterFactory {

    /**
     * Creates a new instance of {@code NamespaceConverter} taking the passed multi-tenancy setting
     * into account.
     *
     * @param multitenant
     *         {@code true} if the created converter should be suitable for multi-tenant
     *         environment,
     *         {@code false} otherwise
     * @return new converter instance
     */
    NamespaceConverter get(boolean multitenant);

    /**
     * Creates an instance of the {@code NsConverterFactory} with the framework default
     * conversion implementation.
     *
     * @return a new instanace of {@code NsConverterFactory} with the default converter
     *         implementations used
     */
    static NsConverterFactory defaults() {
        return DefaultNamespaceConverter::new;
    }
}
