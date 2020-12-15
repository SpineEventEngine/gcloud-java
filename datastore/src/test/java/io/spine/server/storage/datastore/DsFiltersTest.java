/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.truth.IterableSubject;
import io.spine.client.Filter;
import io.spine.client.Filters;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.storage.datastore.given.DsFiltersTestEnv.ArchivedColumn;
import io.spine.server.storage.datastore.given.DsFiltersTestEnv.DeletedColumn;
import io.spine.server.storage.datastore.given.DsFiltersTestEnv.IdStringColumn;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.ge;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt;
import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.le;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.client.CompositeFilter.CompositeOperator.ALL;
import static io.spine.client.CompositeFilter.CompositeOperator.EITHER;
import static io.spine.client.Filters.lt;
import static io.spine.server.entity.storage.TestCompositeQueryParameterFactory.createParams;
import static io.spine.server.storage.LifecycleFlagField.archived;
import static io.spine.server.storage.LifecycleFlagField.deleted;
import static io.spine.server.storage.datastore.DsFilters.fromParams;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;

@DisplayName("`DsFilters` should")
class DsFiltersTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void testPrivateCtor() {
        assertHasPrivateParameterlessCtor(DsFilters.class);
    }

    @Test
    @DisplayName("generate filters from composite query params")
    void testCompositeParams() {
        String idStringValue = "42";
        boolean archivedValue = true;
        boolean deletedValue = true;
        Multimap<Column, Filter> conjunctiveFilters = ImmutableMultimap.of(
                new IdStringColumn(),
                Filters.gt(IdStringColumn.NAME.value(), idStringValue)
        );
        ImmutableMultimap<Column, Filter> disjunctiveFilters = ImmutableMultimap.of(
                new DeletedColumn(),
                Filters.eq(DeletedColumn.NAME.value(), deletedValue),

                new ArchivedColumn(),
                Filters.eq(ArchivedColumn.NAME.value(), archivedValue)
        );
        Collection<CompositeQueryParameter> parameters = ImmutableSet.of(
                createParams(conjunctiveFilters, ALL),
                createParams(disjunctiveFilters, EITHER)
        );

        FilterAdapter columnFilterAdapter = FilterAdapter.of(new DsColumnMapping());
        Collection<StructuredQuery.Filter> filters = fromParams(parameters, columnFilterAdapter);

        IterableSubject assertFilters = assertThat(filters);
        assertFilters.contains(and(gt(IdStringColumn.NAME.value(), idStringValue),
                                   eq(archived.name(), archivedValue)));
        assertFilters.contains(and(gt(IdStringColumn.NAME.value(), idStringValue),
                                   eq(deleted.name(), deletedValue)));
    }

    @Test
    @DisplayName("generate filters from single parameter")
    void testSingleParameter() {
        String versionValue = "314";
        ImmutableMultimap<Column, Filter> singleFilter = ImmutableMultimap.of(
                new IdStringColumn(),
                Filters.le(IdStringColumn.NAME.value(), versionValue)
        );
        Collection<CompositeQueryParameter> parameters = ImmutableSet.of(
                createParams(singleFilter, ALL)
        );

        FilterAdapter columnFilterAdapter = FilterAdapter.of(new DsColumnMapping());
        Collection<StructuredQuery.Filter> filters = fromParams(parameters, columnFilterAdapter);
        IterableSubject assertFilters = assertThat(filters);
        assertFilters.contains(and(le(IdStringColumn.NAME.value(), versionValue)));
    }

    @Test
    @DisplayName("generate filters for multiple disjunctive groups")
    void testMultipleDisjunctiveGroups() {
        String greaterBoundDefiner = "271";
        String standaloneValue = "100";
        String lessBoundDefiner = "42";
        boolean archivedValue = true;
        boolean deletedValue = true;
        Column idStringColumn = new IdStringColumn();
        ImmutableMultimap<Column, Filter> versionFilters = ImmutableMultimap.of(
                idStringColumn, Filters.ge(IdStringColumn.NAME.value(), greaterBoundDefiner),
                idStringColumn, Filters.eq(IdStringColumn.NAME.value(), standaloneValue),
                idStringColumn, lt(IdStringColumn.NAME.value(), lessBoundDefiner)
        );
        ImmutableMultimap<Column, Filter> lifecycleFilters = ImmutableMultimap.of(
                new DeletedColumn(),
                Filters.eq(DeletedColumn.NAME.value(), deletedValue),

                new ArchivedColumn(),
                Filters.eq(DeletedColumn.NAME.value(), archivedValue)
        );
        Collection<CompositeQueryParameter> parameters = ImmutableSet.of(
                createParams(versionFilters, EITHER),
                createParams(lifecycleFilters, EITHER)
        );
        FilterAdapter columnFilterAdapter = FilterAdapter.of(new DsColumnMapping());
        Collection<StructuredQuery.Filter> filters = fromParams(parameters, columnFilterAdapter);
        assertThat(filters).containsExactly(
                and(ge(IdStringColumn.NAME.value(), greaterBoundDefiner),
                    eq(ArchivedColumn.NAME.value(), archivedValue)),
                and(ge(IdStringColumn.NAME.value(), greaterBoundDefiner),
                    eq(DeletedColumn.NAME.value(), deletedValue)),
                and(eq(IdStringColumn.NAME.value(), standaloneValue),
                    eq(ArchivedColumn.NAME.value(), archivedValue)),
                and(eq(IdStringColumn.NAME.value(), standaloneValue),
                    eq(DeletedColumn.NAME.value(), deletedValue)),
                and(PropertyFilter.lt(IdStringColumn.NAME.value(), lessBoundDefiner),
                    eq(ArchivedColumn.NAME.value(), archivedValue)),
                and(PropertyFilter.lt(IdStringColumn.NAME.value(), lessBoundDefiner),
                    eq(DeletedColumn.NAME.value(), deletedValue))
        );
    }

    @Test
    @DisplayName("generate filters from empty params")
    void testEmptyParameters() {
        Collection<CompositeQueryParameter> parameters = Collections.emptySet();
        Collection<StructuredQuery.Filter> filters =
                fromParams(parameters, FilterAdapter.of(new DsColumnMapping()));
        IterableSubject assertFilters = assertThat(filters);
        assertFilters.isNotNull();
        assertFilters.isEmpty();
    }
}
