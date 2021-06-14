/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.datastore.query;

import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.TimestampValue;
import com.google.common.truth.IterableSubject;
import com.google.protobuf.Timestamp;
import io.spine.base.Time;
import io.spine.client.ArchivedColumn;
import io.spine.client.DeletedColumn;
import io.spine.query.QueryPredicate;
import io.spine.query.Subject;
import io.spine.server.storage.datastore.config.DsColumnMapping;
import io.spine.test.storage.StgProject;
import io.spine.test.storage.StgProjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;
import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.le;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.lt;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.datastore.query.DsFilters.fromPredicate;
import static io.spine.test.storage.StgProject.Column.dueDate;
import static io.spine.test.storage.StgProject.Column.idString;
import static io.spine.testing.Assertions.assertHasPrivateParameterlessCtor;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;

@DisplayName("`DsFilters` should")
final class DsFiltersTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void testPrivateCtor() {
        assertHasPrivateParameterlessCtor(DsFilters.class);
    }

    @Test
    @DisplayName("generate filters from query predicates with multiple complex `either()` groups")
    void testCompositeEither() {
        String idStringValue = "42";
        boolean archivedValue = true;
        boolean deletedValue = true;

        StgProject.Query query =
                StgProject.query()
                          .either(project -> project.idString().isGreaterThan(idStringValue)
                                                    .where(ArchivedColumn.is(), archivedValue),
                                  project -> project.idString().isLessThan(idStringValue)
                                                    .where(DeletedColumn.is(), deletedValue))
                          .build();

        FilterAdapter adapter = FilterAdapter.of(new DsColumnMapping());
        Subject<StgProjectId, StgProject> subject = query.subject();
        Collection<StructuredQuery.Filter> filters =
                fromPredicate(subject.predicate(), adapter);

        IterableSubject assertFilters = assertThat(filters);
        String idStringColumnName = idString().name()
                                              .value();
        assertFilters.contains(and(gt(idStringColumnName, idStringValue),
                                   eq(ArchivedColumn.instance()
                                                    .toString(), archivedValue)));
        assertFilters.contains(and(lt(idStringColumnName, idStringValue),
                                   eq(DeletedColumn.instance()
                                                   .toString(), deletedValue)));
    }

    @Test
    @DisplayName("generate filters from query predicates with multiple simple `either()` groups")
    void testFlatEither() {
        String idStringValue = "42";
        Timestamp dueDateValue = Time.currentTime();

        StgProject.Query query =
                StgProject.query()
                          .either(project -> project.idString().isGreaterThan(idStringValue),
                                  project -> project.dueDate().isLessThan(dueDateValue))
                          .build();

        FilterAdapter adapter = FilterAdapter.of(new DsColumnMapping());
        Subject<StgProjectId, StgProject> subject = query.subject();
        Collection<StructuredQuery.Filter> filters =
                fromPredicate(subject.predicate(), adapter);

        IterableSubject assertFilters = assertThat(filters);
        String idStringColumnName = idString().name()
                                              .value();
        TimestampValue expectedDueDate = toTimestampValue(dueDateValue);
        String dueDateColumnName = dueDate().name()
                                            .value();
        assertFilters.contains(gt(idStringColumnName, idStringValue));
        assertFilters.contains(lt(dueDateColumnName, expectedDueDate));
    }

    private static TimestampValue toTimestampValue(Timestamp dueDateValue) {
        return TimestampValue.of(
                ofTimeSecondsAndNanos(dueDateValue.getSeconds(), dueDateValue.getNanos())
        );
    }

    @Test
    @DisplayName("generate filters from a single `Query` predicate")
    void testSingleParameter() {
        String idStringValue = "314";

        StgProject.Query query =
                StgProject.query()
                          .idString()
                          .isLessOrEqualTo(idStringValue)
                          .build();

        FilterAdapter columnFilterAdapter = FilterAdapter.of(new DsColumnMapping());
        Collection<StructuredQuery.Filter> filters =
                fromPredicate(query.subject()
                                   .predicate(), columnFilterAdapter);
        IterableSubject assertFilters = assertThat(filters);
        assertFilters.contains(and(le(idString().name()
                                                .value(), idStringValue)));
    }

    @Test
    @DisplayName("generate filters for a `Query` with a empty predicate")
    void testEmptyParameters() {
        QueryPredicate<StgProject> emptyPredicate =
                StgProject.query()
                          .build()
                          .subject()
                          .predicate();
        Collection<StructuredQuery.Filter> filters =
                fromPredicate(emptyPredicate, FilterAdapter.of(new DsColumnMapping()));
        IterableSubject assertFilters = assertThat(filters);
        assertFilters.isNotNull();
        assertFilters.isEmpty();
    }
}
