/*
 * Copyright 2022, TeamDev. All rights reserved.
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

import io.spine.core.TenantId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.datastore.tenant.NamespaceConverter.NOT_A_TENANT;

@DisplayName("PrefixedNsConverterFactory should")
class PrefixedNsConverterFactoryTest {

    @Test
    @DisplayName("ignore namespaces which do not start from the prefix")
    void ignoreNonMatching() {
        String prefix = "in";
        PrefixedNsConverterFactory factory =
                new PrefixedNsConverterFactory(prefix, NsConverterFactory.defaults());
        NamespaceConverter converter = factory.get(true);
        TenantId matchingTenant = converter.convert(prefix + ".Vstring");
        assertThat(matchingTenant.getValue()).isEqualTo("string");

        TenantId nonMatchingTenant = converter.convert("ABCDE");
        assertThat(nonMatchingTenant).isSameInstanceAs(NOT_A_TENANT);
    }

    @Test
    @DisplayName("ignore namespaces which contain only the prefix")
    void ignoreSolePrefix() {
        String prefix = "pro";
        PrefixedNsConverterFactory factory =
                new PrefixedNsConverterFactory(prefix, NsConverterFactory.defaults());
        NamespaceConverter converter = factory.get(true);
        TenantId nonMatchingTenant = converter.convert(prefix);
        assertThat(nonMatchingTenant).isSameInstanceAs(NOT_A_TENANT);
    }

    @Test
    @DisplayName("ignore namespaces a longer prefix")
    void ignoreLongerPrefix() {
        String prefix = "pre";
        PrefixedNsConverterFactory factory =
                new PrefixedNsConverterFactory(prefix, NsConverterFactory.defaults());
        NamespaceConverter converter = factory.get(true);
        TenantId nonMatchingTenant = converter.convert("pre-processing");
        assertThat(nonMatchingTenant).isSameInstanceAs(NOT_A_TENANT);
    }
}
