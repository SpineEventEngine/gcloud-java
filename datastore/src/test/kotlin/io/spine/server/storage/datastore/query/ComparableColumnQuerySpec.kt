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

import com.google.protobuf.Duration
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.spine.query.RecordQuery
import io.spine.query.RecordQueryBuilder
import io.spine.server.storage.datastore.given.MeasurementColumns.distance
import io.spine.server.storage.datastore.given.MeasurementColumns.span
import io.spine.server.storage.datastore.given.MeasurementColumns.taken_at
import io.spine.server.storage.datastore.given.MeasurementStorage
import io.spine.server.storage.datastore.given.TestEnvironment.singleTenantSpec
import io.spine.test.datastore.comparison.Distance
import io.spine.test.datastore.comparison.Measurement
import io.spine.test.datastore.comparison.MeasurementId
import io.spine.testing.server.storage.datastore.EmulatorTest
import io.spine.testing.server.storage.datastore.TestDatastoreStorageFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Verifies that entity columns of comparable types can be queried with the ordering operators
 * (`>`, `<`, `>=`, `<=`) against the Datastore emulator.
 *
 * Five measurements `m1`..`m5` are stored so that every comparable column
 * ([span] `Duration`, [taken_at] `Timestamp`, and the `(compare_by)` [distance]) increases with
 * the measurement number. Each test then asserts the exact set of matching records, which pins
 * down that the stored column value is compared using the intended semantic ordering — not, say,
 * the lexicographic order of a stringified message.
 */
@EmulatorTest
@DisplayName("Datastore storage should support ordering comparison queries on")
internal class ComparableColumnQuerySpec {

    private val factory = TestDatastoreStorageFactory.local()
    private lateinit var storage: MeasurementStorage

    @BeforeEach
    fun setUp() {
        factory.setUp()
        storage = MeasurementStorage(singleTenantSpec(), factory)
        storage.writeBatch((1..5).map(::measurement))
    }

    @AfterEach
    fun tearDown() {
        factory.tearDown()
    }

    @Nested
    @DisplayName("a `Duration` column")
    inner class DurationColumn {

        @Test
        fun `greater than`() {
            matches { it.where(span).isGreaterThan(seconds(3)) } shouldBe listOf(4, 5)
        }

        @Test
        fun `less than`() {
            matches { it.where(span).isLessThan(seconds(3)) } shouldBe listOf(1, 2)
        }

        @Test
        fun `greater or equal`() {
            matches { it.where(span).isGreaterOrEqualTo(seconds(3)) } shouldBe listOf(3, 4, 5)
        }

        @Test
        fun `less or equal`() {
            matches { it.where(span).isLessOrEqualTo(seconds(3)) } shouldBe listOf(1, 2, 3)
        }

        @Test
        fun `within a range`() {
            matches {
                it.where(span).isGreaterThan(seconds(2))
                it.where(span).isLessOrEqualTo(seconds(4))
            } shouldBe listOf(3, 4)
        }
    }

    @Nested
    @DisplayName("a `(compare_by)` message column")
    inner class CompareByColumn {

        @Test
        fun `greater than`() {
            matches { it.where(distance).isGreaterThan(meters(30)) } shouldBe listOf(4, 5)
        }

        @Test
        fun `less than`() {
            matches { it.where(distance).isLessThan(meters(30)) } shouldBe listOf(1, 2)
        }

        @Test
        fun `greater or equal`() {
            matches { it.where(distance).isGreaterOrEqualTo(meters(30)) } shouldBe listOf(3, 4, 5)
        }

        @Test
        fun `less or equal`() {
            matches { it.where(distance).isLessOrEqualTo(meters(30)) } shouldBe listOf(1, 2, 3)
        }

        @Test
        fun `within a range`() {
            matches {
                it.where(distance).isGreaterThan(meters(20))
                it.where(distance).isLessOrEqualTo(meters(40))
            } shouldBe listOf(3, 4)
        }
    }

    @Nested
    @DisplayName("a `Timestamp` column")
    inner class TimestampColumn {

        @Test
        fun `greater than`() {
            matches { it.where(taken_at).isGreaterThan(minute(3)) } shouldBe listOf(4, 5)
        }

        @Test
        fun `within a range`() {
            matches {
                it.where(taken_at).isGreaterThan(minute(2))
                it.where(taken_at).isLessOrEqualTo(minute(4))
            } shouldBe listOf(3, 4)
        }
    }

    @Nested
    @DisplayName("a column reached through the by-IDs (in-memory) read path")
    inner class InMemoryPath {

        @Test
        fun `filtering the fetched records by a '(compare_by)' column`() {
            matches {
                it.id().`in`(id(2), id(3), id(4))
                it.where(distance).isGreaterThan(meters(20))
            } shouldBe listOf(3, 4)
        }
    }

    /**
     * Runs a query built by [configure] and returns the numbers of the matching measurements.
     */
    private fun matches(
        configure: (RecordQueryBuilder<MeasurementId, Measurement>) -> Unit
    ): List<Int> {
        val builder = RecordQuery.newBuilder(MeasurementId::class.java, Measurement::class.java)
        configure(builder)
        return storage.readAll(builder.build())
            .asSequence()
            .map { it.id.value.removePrefix("m").toInt() }
            .toList()
    }

    /**
     * Asserts that a query returned exactly the given measurement numbers, in any order
     * (a Datastore result set has no guaranteed order unless explicitly sorted).
     */
    private infix fun List<Int>.shouldBe(expected: List<Int>) =
        this shouldContainExactlyInAnyOrder expected

    private companion object {

        private val BASE: Timestamp = Timestamps.fromSeconds(1_600_000_000L)

        private fun measurement(n: Int): Measurement = Measurement.newBuilder()
            .setId(id(n))
            .setTakenAt(minute(n))
            .setSpan(seconds(n))
            .setDistance(meters(n * 10L))
            .build()

        private fun id(n: Int) = MeasurementId.newBuilder().setValue("m$n").build()

        private fun seconds(n: Int): Duration = Durations.fromSeconds(n.toLong())

        private fun minute(n: Int): Timestamp =
            Timestamps.add(BASE, Durations.fromMinutes(n.toLong()))

        private fun meters(m: Long): Distance = Distance.newBuilder().setMeters(m).build()

        private fun meters(m: Int): Distance = meters(m.toLong())
    }
}
