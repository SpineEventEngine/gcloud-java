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

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.client.DatastoreException;
import com.google.rpc.Code;

/**
 * @author Dmytro Dashenkov
 */
public class WriteOperations {

    public static void createOrUpdate(Entity data, DatastoreWrapper datastore) {
        Mutation.Builder mutation = getCreateMutation(data);
        try {
            datastore.commit(mutation);
        } catch (RuntimeException e) {
            if (shouldBeUpdateOperation(e)) {
                mutation = getUpdateMutation(data);
                datastore.commit(mutation);
            } else {
                throw e;
            }
        }

    }

    private static Mutation.Builder getCreateMutation(Entity entity) {
        final Mutation.Builder mutation = Mutation.newBuilder()
                .setInsert(entity);
        return mutation;
    }

    private static Mutation.Builder getUpdateMutation(Entity entity) {
        final Mutation.Builder mutation = Mutation.newBuilder()
                .setUpdate(entity);
        return mutation;
    }

    private static boolean shouldBeUpdateOperation(RuntimeException e) {
        final boolean isDatastoreException = e.getCause() instanceof DatastoreException;

        if (!isDatastoreException) {
            return false;
        }
        final boolean isUpdateOperation = ((DatastoreException) e.getCause()).getCode() == Code.INVALID_ARGUMENT;
        return isUpdateOperation;
    }
}
