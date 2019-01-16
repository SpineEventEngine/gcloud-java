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
import io.spine.server.entity.storage.Column;
import io.spine.server.projection.Projection;
import io.spine.test.datastore.College;
import io.spine.test.datastore.CollegeId;
import io.spine.test.datastore.CollegeVBuilder;
import io.spine.testing.server.entity.given.Given;
import org.checkerframework.checker.nullness.qual.Nullable;

import static io.spine.base.Time.getCurrentTime;

public class CollegeEntity
        extends Projection<CollegeId, College, CollegeVBuilder> {

    private final Timestamp creationTime;

    private CollegeEntity(CollegeId id) {
        super(id);
        this.creationTime = getCurrentTime();
    }

    public static CollegeEntity create(CollegeId id, College state) {
        return Given.projectionOfClass(CollegeEntity.class)
                    .withId(id)
                    .withState(state)
                    .build();
    }

    @Column
    public String getName() {
        return getState().getName();
    }

    @Column
    public @Nullable Integer getStudentCount() {
        int count = getState().getStudentCount();
        return count == 0 ? null : count;
    }

    @Column
    public Timestamp getAdmissionDeadline() {
        return getState().getAdmissionDeadline();
    }

    @Column
    public double getPassingGrade() {
        return getState().getPassingGrade();
    }

    @Column
    public boolean getStateSponsored() {
        return getState().getStateSponsored();
    }

    @Column
    public Timestamp getCreationTime() {
        return creationTime;
    }

    public enum CollegeColumn {
        CREATED("creationTime"),
        NAME("name"),
        STUDENT_COUNT("studentCount"),
        PASSING_GRADE("passingGrade"),
        ADMISSION_DEADLINE("admissionDeadline"),
        STATE_SPONSORED("stateSponsored");

        private final String name;

        CollegeColumn(String name) {
            this.name = name;
        }

        public String columnName() {
            return name;
        }
    }
}
