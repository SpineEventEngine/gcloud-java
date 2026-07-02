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

import io.spine.server.ContextSpec;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorageUnderTest;
import io.spine.server.storage.StorageFactory;
import io.spine.test.datastore.comparison.Measurement;
import io.spine.test.datastore.comparison.MeasurementId;

/**
 * A storage of {@link Measurement} records with {@linkplain MeasurementColumns comparable columns},
 * used to verify ordering comparison queries against a real storage engine.
 */
public final class MeasurementStorage
        extends RecordStorageUnderTest<MeasurementId, Measurement> {

    public MeasurementStorage(ContextSpec context, StorageFactory factory) {
        super(context, factory.createRecordStorage(context, spec()));
    }

    private static RecordSpec<MeasurementId, Measurement> spec() {
        @SuppressWarnings("ConstantConditions")  // Proto getters return non-`null` values.
        var spec = new RecordSpec<>(MeasurementId.class, Measurement.class,
                                    Measurement::getId, MeasurementColumns.definitions());
        return spec;
    }
}
