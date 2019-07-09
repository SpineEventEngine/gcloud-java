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

import io.spine.core.TenantId;
import io.spine.net.EmailAddress;
import io.spine.net.InternetDomain;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.validate.Validate.isDefault;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("`SingleTenantNamespaceSupplier` should")
class SingleTenantNamespaceSupplierTest {

    @Test
    @DisplayName("produce empty namespace")
    void testProduceEmpty() {
        NamespaceSupplier supplier = NamespaceSupplier.singleTenant();
        Namespace namespace = supplier.get();
        assertNotNull(namespace);
        assertThat(namespace.getValue(), isEmptyString());
        TenantId tenantId = namespace.toTenantId();
        assertThat(tenantId, isEffectivelyDefault());
    }

    @Test
    @DisplayName("produce custom namespace")
    void testProduceCustom() {
        String namespaceValue = "my-custom-namespace";
        NamespaceSupplier supplier = NamespaceSupplier.singleTenant(namespaceValue);
        Namespace namespace = supplier.get();
        assertNotNull(namespace);
        assertEquals(namespaceValue, namespace.getValue());

        TenantId tenantId = namespace.toTenantId();
        assertThat(tenantId, not(isEffectivelyDefault()));
        String actualNamespaceValue = tenantId.getValue();
        assertEquals(namespaceValue, actualNamespaceValue);
    }

    @SuppressWarnings("OverlyComplexAnonymousInnerClass") // OK for the test purposes
    private static Matcher<TenantId> isEffectivelyDefault() {
        return new BaseMatcher<TenantId>() {
            @Override
            public boolean matches(Object item) {
                if (!(item instanceof TenantId)) {
                    return false;
                }
                TenantId tenantId = (TenantId) item;
                InternetDomain domain = tenantId.getDomain();
                if (!isDefault(domain)) {
                    return false;
                }
                EmailAddress email = tenantId.getEmail();
                if (!isDefault(email)) {
                    return false;
                }
                String value = tenantId.getValue();
                return value.isEmpty();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("not default, as expected");
            }
        };
    }
}
