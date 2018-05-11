/*
 * Copyright 2018, TeamDev. All rights reserved.
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

package io.spine.web.test;

import io.spine.server.BoundedContext;
import io.spine.server.storage.StorageFactory;

import static io.spine.server.BoundedContext.newName;
import static io.spine.server.storage.memory.InMemoryStorageFactory.newInstance;

/**
 * The test application server.
 *
 * @author Dmytro Dashenkov
 */
final class Server {

    private static final Application app = createApplication();

    /**
     * Prevents the utility class instantiation.
     */
    private Server() {
    }

    /**
     * Retrieves the {@link Application} instance.
     */
    static Application application() {
        return app;
    }

    private static Application createApplication() {
        final String name = "Test Bounded Context";
        final StorageFactory storageFactory = newInstance(newName(name), false);
        final BoundedContext boundedContext =
                BoundedContext.newBuilder()
                              .setName(name)
                              .setStorageFactorySupplier(() -> storageFactory)
                              .build();
        boundedContext.register(new TaskRepository());
        final Application app = Application.create(boundedContext);
        return app;
    }
}
