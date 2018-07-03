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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A value-type representing the GAE project ID.
 *
 * @author Dmytro Dashenkov
 */
public final class ProjectId {

    private final String value;

    private ProjectId(String value) {
        this.value = value;
    }

    /**
     * Creates a new instance of {@code ProjectId} with the passed value.
     *
     * @param projectId the actual project ID value
     * @return new instance of {@code ProjectId}
     */
    public static ProjectId of(String projectId) {
        ProjectId result = new ProjectId(checkNotNull(projectId));
        return result;
    }

    /**
     * Creates new instance of {@code ProjectId} with the value taken from the passed
     * {@linkplain Datastore Datastore config}.
     *
     * @param datastore the {@link Datastore} instance to take the value from
     * @return new instance of {@code ProjectId}
     */
    public static ProjectId of(Datastore datastore) {
        checkNotNull(datastore);
        DatastoreOptions options = datastore.getOptions();
        String value = options.getProjectId();
        return of(value);
    }

    /**
     * @return the string value of the GAE project ID
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectId projectId = (ProjectId) o;
        return Objects.equal(value, projectId.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("value", value)
                          .toString();
    }
}
