/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.StructuredQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A lazy iterator over pages retrieved by reading from Datastore using queries.
 *
 * <p>Each page is represented by a {@link DsQueryIterator}. The pages are formed by performing
 * batch reads restricted to the {@linkplain StructuredQuery#getLimit() query limit}, and reusing
 * the cursor for consequent operations.
 *
 * <p>This iterators contents can be used in paginated manner or combined to a single iterator
 * using the {@code flatMap}.
 *
 * <p>If the limit is not specified, then the page size is determined by the Datastore
 * query restrictions.
 *
 * @param <R>
 *         the type of queried objects
 */
final class DsQueryPageIterator<R> implements Iterator<DsQueryIterator<R>> {

    private final DatastoreWrapper datastore;

    private DsQueryIterator<R> currentPage;
    private @Nullable DsQueryIterator<R> nextPage;

    DsQueryPageIterator(StructuredQuery<R> query, DatastoreWrapper datastore) {
        this.datastore = datastore;
        this.currentPage = datastore.read(query);
    }

    @Override
    public boolean hasNext() {
        if (nextPage == null) {
            nextPage = loadNextPage();
        }
        return nextPage.hasNext();
    }

    @Override
    public DsQueryIterator<R> next() {
        if (nextPage == null) {
            currentPage = loadNextPage();
        } else {
            currentPage = nextPage;
            nextPage = null;
        }
        if (!currentPage.hasNext()) {
            throw new NoSuchElementException("The paginated query results Iterator is empty.");
        }
        return currentPage;
    }

    private DsQueryIterator<R> loadNextPage() {
        StructuredQuery<R> nextPageQuery = currentPage.nextPageQuery();
        return datastore.read(nextPageQuery);
    }
}
