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

import com.google.cloud.datastore.Entity;

/**
 * The container for all the field names we declare in the storage's {@link Entity entities}.
 *
 * <p>Represented by a set of enum containers having their {@link Enum#toString toString()} method overloaded.
 *
 * <p>The values are grouped by their primary usage scope. Though, some of them are used to widely. Those that do are
 * grouped in a separate enum — {@link CommonFields}.
 *
 * @author Dmytro Dashenkov.
 */
class EntityFields {

    enum AggregateFields {

        AGGREGATE_ID;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    enum EventFields {

        PRODUCER_ID,
        EVENT_ID,
        EVENT_TYPE;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    enum EventContextFields {

        CONTEXT_EVENT_ID,
        CONTEXT_TIMESTAMP,
        CONTEXT_OF_COMMAND,
        CONTEXT_VERSION;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    enum EntityStateFields {

        ARCHIVED,
        DELETED;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    enum CommonFields {

        TIMESTAMP,
        TIMESTAMP_NANOS;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }
}
