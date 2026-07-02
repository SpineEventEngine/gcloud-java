/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.spine.query.Columns;
import io.spine.query.RecordColumn;
import io.spine.query.RecordColumns;
import io.spine.test.datastore.comparison.Distance;
import io.spine.test.datastore.comparison.Measurement;

/**
 * Columns of the {@link Measurement} record, each of a comparable type.
 *
 * <ul>
 *     <li>{@code taken_at} — a {@link Timestamp} (ordered via {@code ComparatorRegistry});
 *     <li>{@code span} — a {@link Duration} (ordered via {@code ComparatorRegistry});
 *     <li>{@code distance} — a custom message marked with {@code (compare_by)}.
 * </ul>
 */
@RecordColumns(ofType = Measurement.class)
public final class MeasurementColumns {

    public static final RecordColumn<Measurement, Timestamp> taken_at =
            RecordColumn.create("taken_at", Timestamp.class, Measurement::getTakenAt);

    public static final RecordColumn<Measurement, Duration> span =
            RecordColumn.create("span", Duration.class, Measurement::getSpan);

    public static final RecordColumn<Measurement, Distance> distance =
            RecordColumn.create("distance", Distance.class, Measurement::getDistance);

    /**
     * Prevents instantiation of this holder class.
     */
    private MeasurementColumns() {
    }

    /**
     * Returns all the column definitions.
     */
    public static Columns<Measurement> definitions() {
        return Columns.of(taken_at, span, distance);
    }
}
