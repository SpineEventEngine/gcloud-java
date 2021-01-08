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

package io.spine.server.storage.datastore;

import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import io.spine.server.delivery.CatchUp;
import io.spine.server.delivery.CatchUpId;
import io.spine.server.delivery.CatchUpReadRequest;
import io.spine.server.delivery.CatchUpStorage;
import io.spine.type.TypeUrl;

import java.util.Iterator;

import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.protobuf.util.Timestamps.toNanos;

/**
 * A Google Datastore-backed storage implementation of {@link CatchUpStorage}.
 */
public class DsCatchUpStorage extends DsMessageStorage<CatchUpId, CatchUp, CatchUpReadRequest>
        implements CatchUpStorage {

    protected DsCatchUpStorage(DatastoreWrapper datastore, boolean multitenant) {
        super(datastore, multitenant);
    }

    @Override
    public Iterable<CatchUp> readAll() {
        EntityQuery.Builder builder = Query.newEntityQueryBuilder();
        return readAsIterable(builder);
    }

    private Iterable<CatchUp> readAsIterable(EntityQuery.Builder builder) {
        Iterator<CatchUp> iterator = read(builder);
        return ImmutableList.copyOf(iterator);
    }

    @Override
    public Iterable<CatchUp> readByType(TypeUrl url) {
        EntityQuery.Builder builder = Query.newEntityQueryBuilder();
        builder.setFilter(eq(Column.projectionType.columnName(), url.value()));
        return readAsIterable(builder);
    }

    @Override
    protected  CatchUpId idOf(CatchUp message) {
        return message.getId();
    }

    @Override
    protected  MessageColumn<CatchUp>[] columns() {
        return Column.values();
    }

    /**
     * The columns of the {@code InboxMessage} kind in Datastore.
     */
    private enum Column implements MessageColumn<CatchUp> {

        status("catchup_status", (m) -> {
            return StringValue.of(m.getStatus()
                                   .toString());
        }),

        whenLastRead("when_last_read", (m) -> {
            Timestamp timestamp = m.getWhenLastRead();
            return LongValue.of(toNanos(timestamp));
        }),

        projectionType("projection_type", (m) -> {
            return StringValue.of(m.getId()
                                   .getProjectionType());
        });

        /**
         * The column name.
         */
        private final String name;

        /**
         * Obtains the value of the column from the given message.
         */
        private final Getter<CatchUp> getter;

        Column(String name, Getter<CatchUp> getter) {
            this.name = name;
            this.getter = getter;
        }

        @Override
        public String columnName() {
            return name;
        }

        @Override
        public Getter<CatchUp> getter() {
            return getter;
        }
    }
}
