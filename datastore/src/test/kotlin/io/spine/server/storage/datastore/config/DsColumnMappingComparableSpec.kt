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

import com.google.cloud.datastore.LongValue
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.Value
import com.google.protobuf.Duration
import com.google.protobuf.Message
import com.google.protobuf.util.Durations
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.spine.server.storage.ColumnTypeMapping
import io.spine.string.Stringifiers
import io.spine.test.datastore.comparison.Distance
import io.spine.test.datastore.comparison.MeasurementId
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Tests the [DsColumnMapping] handling of comparable message columns beyond the pre-existing
 * `Timestamp` and `Version` mappings.
 */
@DisplayName("`DsColumnMapping` should")
internal class DsColumnMappingComparableSpec {

    private val mapping = DsColumnMapping()

    private fun store(value: Any): Value<*> {
        @Suppress("UNCHECKED_CAST")
        val rule = mapping.of(value.javaClass) as ColumnTypeMapping<Any, Value<*>>
        return rule.applyTo(value)
    }

    @Nested inner class
    `store a 'Duration'` {

        @Test
        fun `as its total nanoseconds`() {
            val duration = Durations.fromNanos(1_500_000_042L)
            val stored = store(duration)
            stored.shouldBeInstanceOf<LongValue>()
            (stored as LongValue).get() shouldBe 1_500_000_042L
        }

        @Test
        fun `preserving the order of the registered comparator`() {
            val durations = listOf(
                Durations.fromSeconds(-100), Durations.fromMillis(-1), Durations.ZERO,
                Durations.fromNanos(1), Durations.fromMillis(500), Durations.fromSeconds(100)
            )
            val comparator = Durations.comparator()
            for (a in durations) {
                for (b in durations) {
                    val keyOrder = Integer.signum(longOf(a).compareTo(longOf(b)))
                    val valueOrder = Integer.signum(comparator.compare(a, b))
                    withClue("Order of `$a` vs `$b`.") {
                        keyOrder shouldBe valueOrder
                    }
                }
            }
        }

        private fun longOf(d: Duration) = (store(d) as LongValue).get()
    }

    @Nested inner class
    `store a message marked with '(compare_by)'` {

        @Test
        fun `as the order-preserving key produced by 'CompareByEncoder'`() {
            val distance = Distance.newBuilder().setMeters(42).build()
            val stored = store(distance)
            stored.shouldBeInstanceOf<StringValue>()
            (stored as StringValue).get() shouldBe CompareByEncoder.encode(distance)
        }
    }

    @Nested inner class
    `store a message without '(compare_by)'` {

        @Test
        fun `using its string form, as before`() {
            val id: Message = MeasurementId.newBuilder().setValue("m-1").build()
            val stored = store(id)
            stored.shouldBeInstanceOf<StringValue>()
            (stored as StringValue).get() shouldBe Stringifiers.toString(id)
        }
    }
}
