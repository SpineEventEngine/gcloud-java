/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import org.spine3.server.storage.datastore.newapi.DatastoreWrapper;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Objects;

import static com.google.common.collect.Lists.newLinkedList;

/**
 * Iterator for working with datastore and reading large amounts of data with some pagination.
 *
 * @author Mikhail Mikhaylov
 */
/* package */ class PagingDatastoreIterator implements Iterator<EntityResult> {

    private final Query baseQuery;
    private final int pageSize;
    private ByteString endCursor = null;

    private final DatastoreWrapper datastoreWrapper;

    private LinkedList<EntityResult> page = new LinkedList<>();

    /* package */ PagingDatastoreIterator(Query query, DatastoreWrapper datastoreWrapper, int pageSize) {
        this.baseQuery = query;
        this.pageSize = pageSize;
        this.datastoreWrapper = datastoreWrapper;
    }

    @Override
    public boolean hasNext() {
        if (page.isEmpty()) {
            loadData();
        }
        final int pageSize = page.size();
        final boolean result = pageSize > 0 || !Objects.equals(endCursor, ByteString.EMPTY);
        return result;
    }

    @Override
    public EntityResult next() {
        if (!hasNext()) {
            //noinspection NewExceptionWithoutArguments
            throw new NoSuchElementException();
        }
        if (page.isEmpty()) {
            loadData();
        }

        final EntityResult result = page.poll();
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing elements is not supported");
    }

    //TODO:2016-04-08:mikhail.mikhaylov: Check if there is a way to reduce the number of calls.
    private void loadData() {
        final Query.Builder queryBuilder = Query.newBuilder(baseQuery);
        if (endCursor == null) {
            queryBuilder.setStartCursor(queryBuilder.getStartCursor());
        } else if (!endCursor.equals(ByteString.EMPTY)) {
            queryBuilder.setStartCursor(endCursor);
        }

        final Int32Value pagerSizeValue = Int32Value.newBuilder()
                                                    .setValue(pageSize)
                                                    .build();
        queryBuilder.setLimit(pagerSizeValue);

        final QueryResultBatch resultBatch = datastoreWrapper.runQueryForBatch(queryBuilder.build());
        page = newLinkedList();
        if (resultBatch != null) {
            page.addAll(resultBatch.getEntityResultsList());
        }

        //noinspection ConstantConditions // NPE is checked above
        endCursor = page.isEmpty() ? ByteString.EMPTY : resultBatch.getEndCursor();
    }
}
