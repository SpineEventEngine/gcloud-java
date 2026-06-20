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

package io.spine.server.storage.datastore.record

import com.google.cloud.datastore.DoubleValue
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.Key
import com.google.cloud.datastore.KeyValue
import com.google.cloud.datastore.NullValue
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.ints.shouldBeNegative
import io.kotest.matchers.ints.shouldBePositive
import io.kotest.matchers.shouldBe
import io.spine.test.storage.StgProject
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

@DisplayName("`DsEntityComparator` should")
internal class DsEntityComparatorSpec {

    private val idColumn = StgProject.Column.idString().name().value()
    private val statusColumn = StgProject.Column.projectStatusValue().name().value()

    private fun key(name: String): Key =
        Key.newBuilder("test-project", "StgProject", name).build()

    /** A comparator that orders entities ascending by the `idString` column. */
    private fun byIdAscending(): Comparator<Entity> =
        DsEntityComparator.implementing(
            StgProject.query().sortAscendingBy(StgProject.Column.idString()).build().sorting()
        )

    @Test
    fun `order entities ascending by a column value`() {
        val a = Entity.newBuilder(key("a")).set(idColumn, "alpha").build()
        val b = Entity.newBuilder(key("b")).set(idColumn, "beta").build()

        byIdAscending().compare(a, b).shouldBeNegative()
    }

    @Test
    fun `order entities descending when the directive is descending`() {
        val descending = DsEntityComparator.implementing(
            StgProject.query().sortDescendingBy(StgProject.Column.idString()).build().sorting()
        )
        val a = Entity.newBuilder(key("a")).set(idColumn, "alpha").build()
        val b = Entity.newBuilder(key("b")).set(idColumn, "beta").build()

        descending.compare(a, b).shouldBePositive()
    }

    @Test
    fun `treat entities with equal column values as equal`() {
        val a = Entity.newBuilder(key("a")).set(idColumn, "same").build()
        val b = Entity.newBuilder(key("b")).set(idColumn, "same").build()

        byIdAscending().compare(a, b) shouldBe 0
    }

    @Test
    fun `sort an entity with a null column value first`() {
        val withNull = Entity.newBuilder(key("a")).set(idColumn, NullValue.of()).build()
        val withValue = Entity.newBuilder(key("b")).set(idColumn, "beta").build()

        byIdAscending().compare(withNull, withValue) shouldBe -1
    }

    @Test
    fun `sort an entity with a present value before one with a null value`() {
        val withValue = Entity.newBuilder(key("a")).set(idColumn, "alpha").build()
        val withNull = Entity.newBuilder(key("b")).set(idColumn, NullValue.of()).build()

        byIdAscending().compare(withValue, withNull) shouldBe 1
    }

    @Test
    fun `fall back to the next directive when the first compares equal`() {
        val sorting = StgProject.query()
            .sortAscendingBy(StgProject.Column.idString())
            .sortAscendingBy(StgProject.Column.projectStatusValue())
            .build()
            .sorting()
        val comparator = DsEntityComparator.implementing(sorting)

        val a = Entity.newBuilder(key("a")).set(idColumn, "same").set(statusColumn, 1L).build()
        val b = Entity.newBuilder(key("b")).set(idColumn, "same").set(statusColumn, 2L).build()

        comparator.compare(a, b).shouldBeNegative()
    }

    @Test
    fun `reject empty sorting directives`() {
        val noSorting = StgProject.query().build().sorting()

        shouldThrow<NullPointerException> {
            DsEntityComparator.implementing(noSorting)
        }
    }

    @Test
    fun `fail for an unsupported column value type`() {
        val a = Entity.newBuilder(key("a")).set(idColumn, KeyValue.of(key("ref"))).build()
        val b = Entity.newBuilder(key("b")).set(idColumn, "beta").build()

        shouldThrow<IllegalStateException> {
            byIdAscending().compare(a, b)
        }
    }

    @Test
    fun `order entities by a boolean column value`() {
        val internalColumn = StgProject.Column.internal().name().value()
        val byInternalAscending = DsEntityComparator.implementing(
            StgProject.query().sortAscendingBy(StgProject.Column.internal()).build().sorting()
        )
        val falseValued = Entity.newBuilder(key("a")).set(internalColumn, false).build()
        val trueValued = Entity.newBuilder(key("b")).set(internalColumn, true).build()

        byInternalAscending.compare(falseValued, trueValued).shouldBeNegative()
    }

    @Test
    fun `order entities by a double column value`() {
        // Datastore entities are schemaless: a column may hold any value type, and the
        // comparator dispatches on the stored type — here, a `DoubleValue`.
        val a = Entity.newBuilder(key("a")).set(idColumn, DoubleValue.of(1.5)).build()
        val b = Entity.newBuilder(key("b")).set(idColumn, DoubleValue.of(2.5)).build()

        byIdAscending().compare(a, b).shouldBeNegative()
    }
}
