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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A lazy iterator over pages retrieved by reading from Datastore using queries 
 * represented by {@link DsQueryIterator}s. The pages are organized bounded by 
 * {@linkplain StructuredQuery#getLimit() query limit}.
 *
 * <p>This iterators contents can be used in paginated manner or combined to a single iterator 
 * using {@code flatMap}.
 */
class DsQueryPageIterator implements Iterator<DsQueryIterator> {

    private final DatastoreWrapper datastore;
    private DsQueryIterator currentPage;
    private @Nullable DsQueryIterator nextPage;

    DsQueryPageIterator(StructuredQuery<Entity> query, DatastoreWrapper datastore) {
        this.datastore = datastore;
        currentPage = datastore.read(query);
    }

    @Override
    public boolean hasNext() {
        if (nextPage == null) {
            nextPage = loadNextPage();
        }
        return nextPage.hasNext();
    }

    @Override
    public DsQueryIterator next() {
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

    private DsQueryIterator loadNextPage() {
        StructuredQuery<Entity> nextPageQuery = currentPage.nextPageQuery();
        return datastore.read(nextPageQuery);
    }
}
