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

package io.spine.server.storage.datastore.query

import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.Key
import io.kotest.matchers.shouldBe
import io.spine.server.storage.datastore.config.DsColumnMapping
import io.spine.test.storage.StgProject
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Tests [ColumnPredicate] against its documented contract: `test(entity)` returns
 * `true` when the entity matches the parameters of the query subject, and `false`
 * otherwise.
 *
 * The class is reached only by the lookup-by-IDs read path
 * ([io.spine.server.storage.datastore.query.DsLookupByIds]), which filters the
 * fetched entities with `stream.filter(predicate)` — so an inverted result silently
 * keeps the wrong records (see issue #195).
 */
@DisplayName("`ColumnPredicate` should")
internal class ColumnPredicateSpec {

    private val adapter = FilterAdapter.of(DsColumnMapping())

    private val idColumn = StgProject.Column.idString().name().value()
    private val internalColumn = StgProject.Column.internal().name().value()

    private fun predicateFor(query: StgProject.Query) =
        ColumnPredicate(query.subject(), adapter)

    /** Builds an in-memory entity, setting only the columns passed as non-`null`. */
    private fun entity(id: String? = null, internal: Boolean? = null): Entity {
        val builder = Entity.newBuilder(key())
        id?.let { builder.set(idColumn, it) }
        internal?.let { builder.set(internalColumn, it) }
        return builder.build()
    }

    private fun key(): Key = Key.newBuilder("test-project", "StgProject", "entity").build()

    @Test
    fun `reject a 'null' entity`() {
        val predicate = predicateFor(StgProject.query().build())

        predicate.test(null) shouldBe false
    }

    @Test
    fun `match any entity when the query declares no parameters`() {
        val predicate = predicateFor(StgProject.query().build())

        predicate.test(entity(id = "anything")) shouldBe true
    }

    @Nested inner class
    `With a conjunctive (AND) predicate` {

        @Test
        fun `match an entity that satisfies the only parameter`() {
            val predicate = predicateFor(
                StgProject.query().idString().`is`("42").build()
            )

            predicate.test(entity(id = "42")) shouldBe true
        }

        @Test
        fun `reject an entity that violates the only parameter`() {
            val predicate = predicateFor(
                StgProject.query().idString().`is`("42").build()
            )

            predicate.test(entity(id = "99")) shouldBe false
        }

        @Test
        fun `reject an entity that lacks the queried column`() {
            val predicate = predicateFor(
                StgProject.query().idString().`is`("42").build()
            )

            predicate.test(entity(internal = true)) shouldBe false
        }

        @Test
        fun `match an entity that satisfies all parameters`() {
            val predicate = predicateFor(
                StgProject.query().idString().`is`("42").internal().`is`(true).build()
            )

            predicate.test(entity(id = "42", internal = true)) shouldBe true
        }

        @Test
        fun `reject an entity that violates one of the parameters`() {
            val predicate = predicateFor(
                StgProject.query().idString().`is`("42").internal().`is`(true).build()
            )

            predicate.test(entity(id = "42", internal = false)) shouldBe false
        }
    }

    @Nested inner class
    `With a disjunctive (OR) predicate` {

        @Test
        fun `match an entity that satisfies one of the alternatives`() {
            val predicate = predicateFor(
                StgProject.query()
                    .either({ it.idString().`is`("42") }, { it.idString().`is`("7") })
                    .build()
            )

            predicate.test(entity(id = "7")) shouldBe true
        }

        @Test
        fun `reject an entity that satisfies none of the alternatives`() {
            val predicate = predicateFor(
                StgProject.query()
                    .either({ it.idString().`is`("42") }, { it.idString().`is`("7") })
                    .build()
            )

            predicate.test(entity(id = "99")) shouldBe false
        }

        @Test
        fun `match an entity that satisfies one of the alternative groups`() {
            // Multi-parameter `either` branches become child `AND` predicates of the
            // `OR` node, so this exercises the children recursion of the disjunction.
            val predicate = predicateFor(
                StgProject.query()
                    .either(
                        { it.idString().`is`("42").internal().`is`(true) },
                        { it.idString().`is`("7").internal().`is`(false) }
                    )
                    .build()
            )

            predicate.test(entity(id = "42", internal = true)) shouldBe true
        }

        @Test
        fun `reject an entity that satisfies none of the alternative groups`() {
            // A composite `either` yields an `OR` node whose own parameter list is empty
            // (the parameters live in its child `AND` groups), so the disjunction must be
            // driven by those children. The entity is a near miss: it matches the
            // `idString` of the second group but not its `internal` value, so the `AND`
            // within that group rejects it. With no group satisfied, the result is `false`.
            val predicate = predicateFor(
                StgProject.query()
                    .either(
                        { it.idString().`is`("42").internal().`is`(true) },
                        { it.idString().`is`("7").internal().`is`(false) }
                    )
                    .build()
            )

            predicate.test(entity(id = "7", internal = true)) shouldBe false
        }

        @Test
        fun `match any entity for a disjunction with no alternatives`() {
            // A disjunction with neither own parameters nor children imposes no
            // constraint, mirroring the in-memory `RecordQueryMatcher`.
            val predicate = predicateFor(StgProject.query().either().build())

            predicate.test(entity(id = "anything")) shouldBe true
        }

        /**
         * A [ColumnPredicate] over a mixed disjunction:
         * `idString == "42" OR (idString == "7" AND internal == false)`.
         *
         * The single-parameter branch becomes an own parameter of the `OR` node, while the
         * two-parameter branch becomes a child `AND`, so the node carries both. The premise
         * is asserted here so the tests below cannot silently degrade if the query model ever
         * lowers `either(...)` differently.
         */
        private fun mixedDisjunction() = StgProject.query()
            .either(
                { it.idString().`is`("42") },
                { it.idString().`is`("7").internal().`is`(false) }
            )
            .build()
            .also {
                val root = it.subject().predicate()
                check(root.allParams().size == 1 && root.children().size == 1) {
                    "Not a mixed disjunction: ${root.allParams().size} params," +
                        " ${root.children().size} children."
                }
            }
            .let { predicateFor(it) }

        @Test
        fun `match an entity via the own parameter`() {
            mixedDisjunction().test(entity(id = "42")) shouldBe true
        }

        @Test
        fun `match an entity via the child group`() {
            mixedDisjunction().test(entity(id = "7", internal = false)) shouldBe true
        }

        @Test
        fun `reject an entity matching neither the parameter nor the group`() {
            mixedDisjunction().test(entity(id = "999", internal = true)) shouldBe false
        }
    }

    @Nested inner class
    `With a nested predicate` {

        /** A query of the form `internal == true AND (idString == "42" OR idString == "7")`. */
        private fun nestedPredicate() = predicateFor(
            StgProject.query()
                .internal().`is`(true)
                .either({ it.idString().`is`("42") }, { it.idString().`is`("7") })
                .build()
        )

        @Test
        fun `match an entity satisfying the outer parameter and one alternative`() {
            nestedPredicate().test(entity(id = "42", internal = true)) shouldBe true
        }

        @Test
        fun `reject an entity that violates the outer parameter`() {
            nestedPredicate().test(entity(id = "42", internal = false)) shouldBe false
        }

        @Test
        fun `reject an entity that satisfies no inner alternative`() {
            nestedPredicate().test(entity(id = "99", internal = true)) shouldBe false
        }
    }
}
