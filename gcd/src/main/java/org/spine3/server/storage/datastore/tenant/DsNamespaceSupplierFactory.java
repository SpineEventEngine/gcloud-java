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
import org.spine3.annotations.Internal;
import org.spine3.server.storage.datastore.DatastoreStorageFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A factory for the {@link Supplier Supplier&lt;Namespace&gt;} objects.
 *
 * @author Dmytro Dashenkov
 * @see Namespace
 */
@Internal
public class DsNamespaceSupplierFactory {

    private DsNamespaceSupplierFactory() {
        // Avoid initializing of a utility class
    }

    /**
     * Produces an instance of {@link Supplier Supplier&lt;Namespace&gt;} for the given
     * {@link DatastoreStorageFactory}.
     *
     * @param factory the factory to poduce the supplier for
     * @return a {@link Supplier Supplier&lt;Namespace&gt;} instance depending on the given
     * {@link DatastoreStorageFactory}
     * {@linkplain org.spine3.server.storage.StorageFactory#isMultitenant multitenancy mode}
     */
    public static Supplier<Namespace> getSupplierFor(DatastoreStorageFactory factory) {
        checkNotNull(factory);
        final Supplier<Namespace> result = NamespaceSupplier.instanceFor(factory);
        return result;
    }
}
