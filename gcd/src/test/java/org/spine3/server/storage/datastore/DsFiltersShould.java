/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.junit.Test;
import org.spine3.base.Version;
import org.spine3.client.ColumnFilter;
import org.spine3.server.entity.Entity;
import org.spine3.server.entity.EntityWithLifecycle;
import org.spine3.server.entity.VersionableEntity;
import org.spine3.server.entity.storage.Column;
import org.spine3.server.entity.storage.CompositeQueryParameter;

import java.lang.reflect.Method;
import java.util.Collection;

import static com.google.cloud.datastore.StructuredQuery.CompositeFilter.and;
import static com.google.cloud.datastore.StructuredQuery.Filter;
import static com.google.common.collect.ImmutableMultimap.of;
import static org.junit.Assert.fail;
import static org.spine3.client.ColumnFilters.eq;
import static org.spine3.client.CompositeColumnFilter.CompositeOperator.ALL;
import static org.spine3.client.CompositeColumnFilter.CompositeOperator.EITHER;
import static org.spine3.server.entity.storage.TestCompositeQueryParameterFactory.createParams;
import static org.spine3.server.storage.EntityField.version;
import static org.spine3.server.storage.LifecycleFlagField.archived;
import static org.spine3.server.storage.LifecycleFlagField.deleted;
import static org.spine3.server.storage.datastore.type.DatastoreTypeRegistryFactory.defaultInstance;
import static org.spine3.test.Tests.assertHasPrivateParameterlessCtor;
import static org.spine3.test.Verify.assertContainsAll;

/**
 * @author Dmytro Dashenkov
 */
public class DsFiltersShould {

    private static final String VERSION_GETTER_NAME = "getVersion";
    private static final String DELETED_GETTER_NAME = "isDeleted";
    private static final String ARCHIVED_GETTER_NAME = "isArchived";

    @Test
    public void have_private_util_ctor() {
        assertHasPrivateParameterlessCtor(DsFilters.class);
    }

    @Test
    public void generate_filters_from_composite_query_params() {
        final int versionValue = 42;
        final Version versionColumnValue = Version.newBuilder()
                                                  .setNumber(versionValue)
                                                  .build();
        final boolean archivedValue = true;
        final boolean deletedValue = true;
        final Multimap<Column, ColumnFilter> conjunctiveFilters = of(
                column(VersionableEntity.class, VERSION_GETTER_NAME), eq(version.name(),
                                                                         versionColumnValue)
        );
        final Multimap<Column, ColumnFilter> disjunctiveFilters = of(
                column(EntityWithLifecycle.class, DELETED_GETTER_NAME), eq(deleted.name(),
                                                                           deletedValue),
                column(EntityWithLifecycle.class, ARCHIVED_GETTER_NAME), eq(archived.name(),
                                                                            archivedValue)
        );
        final Collection<CompositeQueryParameter> parameter = ImmutableSet.of(
                createParams(conjunctiveFilters, ALL),
                createParams(disjunctiveFilters, EITHER)
        );

        final ColumnHandler columnHandler = ColumnHandler.wrap(defaultInstance());
        final Collection<Filter> filters = DsFilters.fromParams(parameter, columnHandler);
        assertContainsAll(filters, and(PropertyFilter.eq(version.name(), versionValue),
                                       PropertyFilter.eq(archived.name(), archivedValue)),
                                   and(PropertyFilter.eq(version.name(), versionValue),
                                       PropertyFilter.eq(deleted.name(), deletedValue)));
    }

    @Test
    public void generate_filters_from_single_parameter() {
        final int versionValue = 314;
        final Version versionColumnValue = Version.newBuilder()
                                                  .setNumber(versionValue)
                                                  .build();
        final Multimap<Column, ColumnFilter> singleFilter = of(
                column(VersionableEntity.class, VERSION_GETTER_NAME), eq(version.name(),
                                                                         versionColumnValue)
        );
        final Collection<CompositeQueryParameter> parameter = ImmutableSet.of(
                createParams(singleFilter, ALL)
        );

        final ColumnHandler columnHandler = ColumnHandler.wrap(defaultInstance());
        final Collection<Filter> filters = DsFilters.fromParams(parameter, columnHandler);
        assertContainsAll(filters, PropertyFilter.eq(version.name(), versionValue));
    }

    @Test
    public void generate_filters_for_two_disjunctive_groups() {
        final int versionValue1 = 271;
        final int versionValue2 = 1;
        final Version versionColumnValue1 = Version.newBuilder()
                                                   .setNumber(versionValue1)
                                                   .build();
        final Version versionColumnValue2 = Version.newBuilder()
                                                   .setNumber(versionValue2)
                                                   .build();
        final boolean archivedValue = true;
        final boolean deletedValue = true;
        final Multimap<Column, ColumnFilter> versionFilters = of(
                column(VersionableEntity.class, VERSION_GETTER_NAME), eq(version.name(),
                                                                         versionColumnValue1),
                column(VersionableEntity.class, VERSION_GETTER_NAME), eq(version.name(),
                                                                         versionColumnValue2)
        );
        final Multimap<Column, ColumnFilter> lifecycleFilters = of(
                column(EntityWithLifecycle.class, DELETED_GETTER_NAME), eq(deleted.name(),
                                                                           deletedValue),
                column(EntityWithLifecycle.class, ARCHIVED_GETTER_NAME), eq(archived.name(),
                                                                            archivedValue)
        );
        final Collection<CompositeQueryParameter> parameter = ImmutableSet.of(
                createParams(versionFilters, EITHER),
                createParams(lifecycleFilters, EITHER)
        );

        final ColumnHandler columnHandler = ColumnHandler.wrap(defaultInstance());
        final Collection<Filter> filters = DsFilters.fromParams(parameter, columnHandler);
        assertContainsAll(filters, and(PropertyFilter.eq(version.name(), versionValue1),
                                       PropertyFilter.eq(archived.name(), archivedValue)),
                                   and(PropertyFilter.eq(version.name(), versionValue1),
                                       PropertyFilter.eq(deleted.name(), deletedValue)),
                                   and(PropertyFilter.eq(version.name(), versionValue2),
                                       PropertyFilter.eq(archived.name(), archivedValue)),
                                   and(PropertyFilter.eq(version.name(), versionValue2),
                                       PropertyFilter.eq(deleted.name(), deletedValue)));
    }

    private static Column column(Class<? extends Entity> cls, String methodName) {
        Method method = null;
        try {
            method = cls.getMethod(methodName);
        } catch (NoSuchMethodException e) {
            fail("Method " + methodName + " not found.");
        }
        final Column column = Column.from(method);
        return column;
    }
}
