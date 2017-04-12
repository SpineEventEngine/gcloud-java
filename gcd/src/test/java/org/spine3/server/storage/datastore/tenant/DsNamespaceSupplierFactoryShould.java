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

import com.google.common.base.Supplier;
import org.junit.Test;
import org.spine3.server.storage.datastore.DatastoreStorageFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.spine3.test.Tests.assertHasPrivateParameterlessCtor;

/**
 * @author Dmytro Dashenkov
 */
public class DsNamespaceSupplierFactoryShould {

    @Test
    public void have_private_utility_ctor() {
        assertHasPrivateParameterlessCtor(DsNamespaceSupplierFactory.class);
    }

    @Test
    public void retrieve_single_tenant_supplier() {
        final DatastoreStorageFactory factory = mock(DatastoreStorageFactory.class);
        when(factory.isMultitenant()).thenReturn(false);
        final Supplier<Namespace> supplier = DsNamespaceSupplierFactory.getSupplierFor(factory);
        assertThat(supplier, instanceOf(SingleTenantNamespaceSupplier.class));
    }

    @Test
    public void retrieve_multitenant_supplier() {
        final DatastoreStorageFactory factory = mock(DatastoreStorageFactory.class);
        when(factory.isMultitenant()).thenReturn(true);
        final Supplier<Namespace> supplier = DsNamespaceSupplierFactory.getSupplierFor(factory);
        assertThat(supplier, instanceOf(MultitenantNamespaceSupplier.class));
    }
}
