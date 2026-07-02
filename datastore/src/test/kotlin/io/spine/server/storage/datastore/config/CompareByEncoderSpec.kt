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

package io.spine.server.storage.datastore.config

import com.google.protobuf.Duration
import com.google.protobuf.Int64Value
import com.google.protobuf.Message
import com.google.protobuf.Timestamp
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.spine.test.datastore.comparison.Assorted
import io.spine.test.datastore.comparison.Countdown
import io.spine.test.datastore.comparison.Distance
import io.spine.test.datastore.comparison.Interval
import io.spine.test.datastore.comparison.Ranking
import io.spine.test.datastore.comparison.Score
import io.spine.test.datastore.comparison.Tier
import io.spine.test.datastore.comparison.Waypoint
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Tests that [CompareByEncoder] produces order-preserving keys: for any two messages `a` and `b`,
 * the lexicographical ordering of their encoded keys matches the ordering imposed by the message's
 * own generated `compareTo` (the `(compare_by)` contract).
 *
 * The keys are compared as plain strings, which is exactly how the Datastore and the in-memory
 * read paths compare the stored column values.
 */
@DisplayName("`CompareByEncoder` should produce a key that preserves the order of")
internal class CompareByEncoderSpec {

    @Nested inner class
    `a single-field message` {

        @Test
        fun `covering the whole 'long' range`() {
            val values = longs().map { distance(it) }
            assertOrderPreserved(values)
        }
    }

    @Nested inner class
    `a multi-field message` {

        @Test
        fun `comparing the first field before the second`() {
            val values = buildList {
                for (points in ints()) {
                    for (name in strings()) {
                        add(score(points, name))
                    }
                }
            }
            assertOrderPreserved(values)
        }
    }

    @Nested inner class
    `a descending message` {

        @Test
        fun `reversing the natural order`() {
            val values = (-5..5).map { countdown(it) }
            assertOrderPreserved(values)
        }
    }

    @Nested inner class
    `a message compared via 'ComparatorRegistry' fields` {

        @Test
        fun `ordering by 'Timestamp' then 'Duration'`() {
            val values = buildList {
                for (t in timestamps()) {
                    for (d in durations()) {
                        add(interval(t, d))
                    }
                }
            }
            assertOrderPreserved(values)
        }
    }

    @Nested inner class
    `a message with nested comparable fields` {

        @Test
        fun `recursing into each nested message`() {
            val values = buildList {
                for (meters in longs()) {
                    for (points in ints()) {
                        add(ranking(meters, points))
                    }
                }
            }
            assertOrderPreserved(values)
        }
    }

    @Nested inner class
    `a message mixing 'double', 'bool', 'enum' and wrapper fields` {

        @Test
        fun `ordering each field kind`() {
            val values = buildList {
                for (ratio in listOf(-2.5, -0.0, 0.0, 1.5)) {
                    for (flag in listOf(false, true)) {
                        for (tier in listOf(Tier.BRONZE, Tier.SILVER, Tier.GOLD)) {
                            for (amount in listOf(-3L, 7L)) {
                                add(assorted(ratio, flag, tier, amount))
                            }
                        }
                    }
                }
            }
            assertOrderPreserved(values)
        }
    }

    @Nested inner class
    `a message ordered by a dotted field path` {

        @Test
        fun `reaching the nested leaf value`() {
            val values = longs().map { waypoint(it) }
            assertOrderPreserved(values)
        }
    }

    companion object {

        private fun longs() =
            listOf(Long.MIN_VALUE, -1_000_000L, -42L, -1L, 0L, 1L, 42L, 1_000_000L, Long.MAX_VALUE)

        private fun ints() = listOf(Int.MIN_VALUE, -7, -1, 0, 1, 7, Int.MAX_VALUE)

        private fun strings() = listOf("", "A", "Ab", "a", "ab", "abc", "b")

        private fun timestamps() = listOf(
            timestamp(-100, 0), timestamp(-1, 999_999_999), timestamp(0, 0),
            timestamp(0, 1), timestamp(1, 0), timestamp(100, 500)
        )

        private fun durations() = listOf(
            duration(-100, 0), duration(-1, -500_000_000), duration(0, -1),
            duration(0, 0), duration(0, 1), duration(1, 500_000_000), duration(100, 0)
        )

        private fun distance(meters: Long) = Distance.newBuilder().setMeters(meters).build()

        private fun score(points: Int, name: String) =
            Score.newBuilder().setPoints(points).setName(name).build()

        private fun countdown(value: Int) = Countdown.newBuilder().setValue(value).build()

        private fun interval(start: Timestamp, length: Duration) =
            Interval.newBuilder().setStart(start).setLength(length).build()

        private fun ranking(meters: Long, points: Int) = Ranking.newBuilder()
            .setDistance(distance(meters))
            .setScore(score(points, "r"))
            .build()

        private fun assorted(ratio: Double, flag: Boolean, tier: Tier, amount: Long) =
            Assorted.newBuilder()
                .setRatio(ratio)
                .setFlag(flag)
                .setTier(tier)
                .setAmount(Int64Value.of(amount))
                .build()

        private fun waypoint(meters: Long) =
            Waypoint.newBuilder().setPoint(distance(meters)).build()

        private fun timestamp(seconds: Long, nanos: Int) =
            Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build()

        private fun duration(seconds: Long, nanos: Int) =
            Duration.newBuilder().setSeconds(seconds).setNanos(nanos).build()

        /**
         * Asserts that, for every pair of the given comparable messages, the sign of comparing
         * their encoded keys equals the sign of the messages' own `compareTo`.
         */
        private fun <T : Comparable<T>> assertOrderPreserved(values: List<T>) {
            for (a in values) {
                for (b in values) {
                    val keyOrder = Integer.signum(
                        CompareByEncoder.encode(a as Message)
                            .compareTo(CompareByEncoder.encode(b as Message))
                    )
                    val valueOrder = Integer.signum(a.compareTo(b))
                    withClue("Order of `$a` vs `$b`.") {
                        keyOrder shouldBe valueOrder
                    }
                }
            }
        }
    }
}
