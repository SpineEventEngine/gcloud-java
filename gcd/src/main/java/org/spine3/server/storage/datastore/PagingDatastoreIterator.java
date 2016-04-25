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

import com.google.api.services.datastore.DatastoreV1;
import com.google.protobuf.ByteString;

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
/* package */ class PagingDatastoreIterator implements Iterator<DatastoreV1.EntityResult> {

    private final DatastoreV1.Query baseQuery;
    private final int pageSize;
    private ByteString endCursor = null;

    private final DatastoreWrapper datastoreWrapper;

    private LinkedList<DatastoreV1.EntityResult> page = new LinkedList<>();

    /* package */ PagingDatastoreIterator(DatastoreV1.Query query, DatastoreWrapper datastoreWrapper, int pageSize) {
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
        return pageSize > 0 || !Objects.equals(endCursor, ByteString.EMPTY);
    }

    @Override
    public DatastoreV1.EntityResult next() {
        if (!hasNext()) {
            //noinspection NewExceptionWithoutArguments
            throw new NoSuchElementException();
        }
        if (page.isEmpty()) {
            loadData();
        }

        final DatastoreV1.EntityResult result = page.poll();
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing elements is not supported");
    }

    //TODO:2016-04-08:mikhail.mikhaylov: Check if there is a way to reduce the number of calls.
    private void loadData() {
        final DatastoreV1.Query.Builder queryBuilder = DatastoreV1.Query.newBuilder(baseQuery);
        if (endCursor == null) {
            queryBuilder.setStartCursor(queryBuilder.getStartCursor());
        } else if (!endCursor.equals(ByteString.EMPTY)) {
            queryBuilder.setStartCursor(endCursor);
        }

        queryBuilder.setLimit(pageSize);

        final DatastoreV1.QueryResultBatch resultBatch = datastoreWrapper.runQueryForBatch(queryBuilder.build());
        page = newLinkedList();
        if (resultBatch != null) {
            page.addAll(resultBatch.getEntityResultList());
        }

        //noinspection ConstantConditions // NPE is checked above
        endCursor = page.isEmpty() ? ByteString.EMPTY : resultBatch.getEndCursor();
    }
}
