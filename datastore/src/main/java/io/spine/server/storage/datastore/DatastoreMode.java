/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore;

/**
 * A mode in which Google Datastore operates.
 */
public enum DatastoreMode {

    /**
     * Native Datastore mode.
     *
     * <p>In this mode, Datastore is mostly eventually consistent. Also it has significant
     * <a href="https://cloud.google.com/datastore/docs/concepts/limits#Cloud_Datastore_limits">limitations</a>
     * on accessing the records.
     *
     * <p>Since recently, the projects in Google Cloud cannot be created in this mode.
     * So this one works primarily for the legacy applications.
     */
    NATIVE,

    /**
     * Firestore in Datastore mode.
     *
     * <p>In this mode, Datastore becomes strongly consistent in most cases. However, some limits
     * are still <a href="https://cloud.google.com/datastore/docs/concepts/limits#limits">applied.</a>
     *
     * <p>All new Cloud projects provide this mode as a default one.
     */
    FIRESTORE_AS_DATASTORE
}
