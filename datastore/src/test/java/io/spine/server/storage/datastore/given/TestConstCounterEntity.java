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

package io.spine.server.storage.datastore.given;

import com.google.protobuf.Timestamp;
import io.spine.base.Time;
import io.spine.core.Version;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.entity.storage.Column;
import io.spine.server.projection.Projection;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.testing.server.entity.given.Given;

import static io.spine.base.Time.currentTime;

public class TestConstCounterEntity
        extends Projection<ProjectId, Project, Project.Builder> {

    private static final int COUNTER = 42;

    private final Timestamp creationTime;
    private LifecycleFlags lifecycleFlags;

    private TestConstCounterEntity(ProjectId id) {
        super(id);
        this.creationTime = currentTime();
    }

    public static TestConstCounterEntity create(ProjectId id) {
        return Given.projectionOfClass(TestConstCounterEntity.class)
                    .withId(id)
                    .build();
    }

    public static TestConstCounterEntity create(ProjectId id, Project state) {
        return Given.projectionOfClass(TestConstCounterEntity.class)
                    .withId(id)
                    .withState(state)
                    .build();
    }

    @Column
    public int getCounter() {
        return COUNTER;
    }

    @Column
    public long getBigCounter() {
        return getCounter();
    }

    @Column
    public boolean isCounterEven() {
        return true;
    }

    @Column
    public String getCounterName() {
        return id().toString();
    }

    @Column
    public Version getCounterVersion() {
        return Version.newBuilder()
                      .setNumber(COUNTER)
                      .setTimestamp(Time.currentTime())
                      .vBuild();
    }

    @Column
    public Timestamp getCreationTime() {
        return creationTime;
    }

    @Column
    public Project getCounterState() {
        return state();
    }

    @Override
    public LifecycleFlags getLifecycleFlags() {
        return lifecycleFlags == null ? super.getLifecycleFlags() : lifecycleFlags;
    }

    public void injectLifecycle(LifecycleFlags flags) {
        this.lifecycleFlags = flags;
    }
}
