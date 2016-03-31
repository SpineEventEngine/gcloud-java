/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

package org.spine3.sample.gae;

import org.spine3.server.storage.datastore.DatastoreStorageFactory;
import org.spine3.server.storage.datastore.LocalDatastoreStorageFactory;

/**
 * A sample application showing basic usage of Spine framework running on GAE local Datastore.
 *
 * @author Alexander Litus
 */
public class Application extends org.spine3.examples.aggregate.server.Application {

    /**
     * Creates a new sample with the specified storage factory.
     *
     * @param storageFactory factory used to create and set up storages.
     */
    @SuppressWarnings("TypeMayBeWeakened") // not in this case
    public Application(DatastoreStorageFactory storageFactory) {
        super(storageFactory);
    }

    /**
     * The entry point of the sample.
     *
     * See instructions on configuring local Datastore environment on this page:
     * https://github.com/SpineEventEngine/gae-java/wiki/Configuring-Local-Datastore-Environment
     */
    public static void main(String[] args) {
        final LocalDatastoreStorageFactory storageFactory = LocalDatastoreStorageFactory.getDefaultInstance();
//        storageFactory.setUp();
        final Application app = new Application(storageFactory);
        app.execute();
//        storageFactory.tearDown();
    }
}
