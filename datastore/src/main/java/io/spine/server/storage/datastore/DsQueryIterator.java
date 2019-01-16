/*
 * Copyright 2019, TeamDev. All rights reserved.
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
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.UnmodifiableIterator;

import java.util.NoSuchElementException;

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
 */
final class DsQueryIterator extends UnmodifiableIterator<Entity> {

    private final StructuredQuery<Entity> query;
    private final QueryResults<Entity> currentPage;

    private final Integer limit;
    private int readCount = 0;

    private boolean terminated;

    DsQueryIterator(StructuredQuery<Entity> query, DatastoreReaderWriter datastore) {
        super();
        this.query = query;
        this.limit = query.getLimit();
        this.currentPage = datastore.run(query);
    }

    @Override
    public boolean hasNext() {
        if (terminated) {
            return false;
        }
        if (limitMet()) {
            terminate();
            return false;
        }
        if (!currentPage.hasNext()) {
            terminate();
            return false;
        }
        return true;
    }

    private boolean limitMet() {
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
    StructuredQuery<Entity> nextPageQuery() {
        Cursor cursorAfter = currentPage.getCursorAfter();
        StructuredQuery<Entity> queryForMoreResults =
                query.toBuilder()
                     .setStartCursor(cursorAfter)
                     .build();
        return queryForMoreResults;
    }

    @Override
    public Entity next() {
        if (!hasNext()) {
            throw new NoSuchElementException("The query results Iterator is empty.");
        }
        Entity result = currentPage.next();
        readCount++;
        return result;
    }
}
