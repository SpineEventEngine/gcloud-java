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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.spine3.net.EmailAddress;
import org.spine3.net.InternetDomain;
import org.spine3.server.storage.datastore.ProjectId;
import org.spine3.users.TenantId;

import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.spine3.validate.Validate.isDefault;

/**
 * @author Dmytro Dashenkov
 */
public class SingleTenantNamespaceSuppierShould {

    @Test
    public void produce_empty_namespace() {
        final NamespaceSupplier supplier = NamespaceSupplier.instance(false,
                                                                      null,
                                                                      ProjectId.of("any"));
        final Namespace namespace = supplier.get();
        assertNotNull(namespace);
        assertThat(namespace.getValue(), isEmptyString());
        final TenantId tenantId = namespace.toTenantId();
        assertThat(tenantId, isEffectivelyDefault());
    }

    @Test
    public void produce_custom_namespace() {
        final String namespaceValue = "my-custom-namespace";
        final NamespaceSupplier supplier = NamespaceSupplier.instance(false,
                                                                      namespaceValue,
                                                                      ProjectId.of("some"));
        final Namespace namespace = supplier.get();
        assertNotNull(namespace);
        assertEquals(namespaceValue, namespace.getValue());

        final TenantId tenantId = namespace.toTenantId();
        assertThat(tenantId, not(isEffectivelyDefault()));
        final String actualNamespaceValue = tenantId.getValue();
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
                final TenantId tenantId = (TenantId) item;
                final InternetDomain domain = tenantId.getDomain();
                if (!isDefault(domain)) {
                    return false;
                }
                final EmailAddress email = tenantId.getEmail();
                if (!isDefault(email)) {
                    return false;
                }
                final String value = tenantId.getValue();
                return value.isEmpty();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("not default, as expected");
            }
        };
    }
}
