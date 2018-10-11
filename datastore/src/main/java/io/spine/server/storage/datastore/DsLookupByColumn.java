/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.Streams;
import com.google.protobuf.FieldMask;
import io.spine.client.OrderBy;
import io.spine.server.entity.EntityRecord;
import io.spine.type.TypeUrl;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import static io.spine.server.storage.datastore.DsEntityComparator.implementing;
import static io.spine.server.storage.datastore.DsQueryHelper.maskRecord;

/**
 * An {@code Entity} lookup in Google Datastore using {@code Entity}
 * {@link io.spine.server.entity.storage.Column columns}.
 *
 * @author Mykhailo Drachuk
 */
final class DsLookupByColumn<I> {

    private final DatastoreWrapper datastore;
    private final TypeUrl typeUrl;

    DsLookupByColumn(DatastoreWrapper datastore, TypeUrl url) {
        this.datastore = datastore;
        this.typeUrl = url;
    }

    /**
     * Performs the given Datastore {@linkplain com.google.cloud.datastore.StructuredQuery queries}
     * and combines results into
     * a single lazy iterator.
     *
     * <p>The resulting iterator is constructed of
     * {@linkplain DatastoreWrapper#read(com.google.cloud.datastore.StructuredQuery) Datastore query
     * response iterators}
     * concatenated together one by one. Each of them is evaluated only after the previous one runs
     * out of records (i.e. {@code hasNext()} method returns {@code false}). The order of
     * the iterators corresponds to the order of the {@code queries}.
     *
     * @param queries
     *         the queries to perform
     * @param fieldMask
     *         the {@code FieldMask} to apply to all the retrieved entity states
     * @return an iterator over the resulting entity records
     */
    Iterator<EntityRecord> execute(Collection<StructuredQuery<Entity>> queries,
                                   FieldMask fieldMask) {
        return read(queries)
                .map(DsQueryHelper::toRecord)
                .map(maskRecord(typeUrl, fieldMask))
                .iterator();
    }

    Iterator<EntityRecord> execute(Collection<StructuredQuery<Entity>> queries,
                                   OrderBy orderBy, FieldMask fieldMask) {
        return read(queries)
                .sorted(implementing(orderBy))
                .map(DsQueryHelper::toRecord)
                .map(maskRecord(typeUrl, fieldMask))
                .iterator();
    }

    Iterator<EntityRecord> execute(Collection<StructuredQuery<Entity>> queries,
                                   OrderBy orderBy, int limit, FieldMask fieldMask) {
        return read(queries)
                .sorted(implementing(orderBy))
                .limit(limit)
                .map(DsQueryHelper::toRecord)
                .map(maskRecord(typeUrl, fieldMask))
                .iterator();
    }

    private Stream<Entity> read(Collection<StructuredQuery<Entity>> queries) {
        return queries.stream()
                      .map(datastore::read)
                      .flatMap(Streams::stream);
    }
}
