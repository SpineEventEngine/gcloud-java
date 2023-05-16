/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.Cursor;
import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.UnmodifiableIterator;
import io.spine.annotation.Internal;
import io.spine.logging.Logging;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An {@code Iterator} over the {@link com.google.cloud.datastore.StructuredQuery} results.
 *
 * <p>This {@code Iterator} loads the results lazily by evaluating
 * the {@link com.google.cloud.datastore.QueryResults} and performing cursor queries.
 *
 * <p>The first query to the datastore is performed on creating an instance of
 * the {@code Iterator}.
 *
 * <p>A call to {@link #hasNext() hasNext()} may cause a query to the Datastore if the current
 * {@linkplain com.google.cloud.datastore.QueryResults results page} is fully processed.
 *
 * <p>A call to {@link #next() next()} may not cause a Datastore query.
 *
 * <p>The {@link #remove() remove()} method throws an {@link UnsupportedOperationException}.
 *
 * @param <R>
 *         the type of queried objects
 */
@Internal
public final class DsQueryIterator<R> extends UnmodifiableIterator<R> implements Logging {

    private final StructuredQuery<R> query;
    private final QueryResults<R> currentPage;

    private final Integer limit;
    private int readCount = 0;

    private boolean terminated;

    DsQueryIterator(StructuredQuery<R> query, DatastoreReader datastore) {
        super();
        this.query = checkNotNull(query);
        this.limit = query.getLimit();
        this.currentPage = checkNotNull(datastore).run(query);
    }

    @Override
    public boolean hasNext() {
        if (terminated) {
            return false;
        }
        if (limitReached()) {
            terminate();
            return false;
        }
        if (!currentPage.hasNext()) {
            terminate();
            return false;
        }
        return true;
    }

    private boolean limitReached() {
        return limit != null && readCount >= limit;
    }

    private void terminate() {
        terminated = true;
    }

    /**
     * Creates a query to the next batch of entities.
     *
     * <p>The query is built utilizing the {@linkplain Cursor Datastore Cursor} from the current
     * query results.
     */
    StructuredQuery<R> nextPageQuery() {
        Cursor cursorAfter = currentPage.getCursorAfter();
        StructuredQuery<R> queryForMoreResults =
                query.toBuilder()
                     .setStartCursor(cursorAfter)
                     .build();
        return queryForMoreResults;
    }

    @Override
    public R next() {
        if (!hasNext()) {
            throw new NoSuchElementException("The query results Iterator is empty.");
        }
        R result = currentPage.next();
        readCount++;
        return result;
    }
}
