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

import com.google.cloud.datastore.Datastore;
import org.spine3.annotations.Internal;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A validator of the Datastore {@linkplain Namespace namespaces}.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public class DsNamespaceValidator {

    private final NamespaceIndex namespaceIndex;

    /**
     * Creates a new instance of the {@code DsNamespaceValidator}.
     *
     * @param datastore the {@link Datastore} to validate the {@linkplain Namespace namespaces} upon
     */
    public DsNamespaceValidator(Datastore datastore, boolean multitenant) {
        this.namespaceIndex = new NamespaceIndex(datastore, multitenant);
    }

    /**
     * Validates the given {@link Namespace} to match these constraints:
     * <ul>
     *     <li>be non-{@code null};
     *     <li>be {@linkplain NamespaceIndex#contains(Namespace) present} in the Datastore.
     * </ul>
     *
     * @param namespace the {@link Namespace} to validate
     * @throws IllegalStateException upon an invalid {@link Namespace}
     */
    public void validate(Namespace namespace) throws IllegalStateException {
        checkNotNull(namespace);
        final boolean found = namespaceIndex.contains(namespace);
        checkArgument(found,
                      "Namespace %s could not be found in the Datastore.",
                      namespace);
    }
}
