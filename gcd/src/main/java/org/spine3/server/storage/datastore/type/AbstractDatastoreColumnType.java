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

package org.spine3.server.storage.datastore.type;

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Value;
import org.spine3.annotation.SPI;

/**
 * An abstract base for implementing {@link DatastoreColumnType}.
 *
 * <p>This class provides the default implementation for the {@link #setNull setNull} method
 * of {@link org.spine3.server.entity.storage.ColumnType}. Since this method implementation is the
 * same for all the types within a Storage implementation, it's convenient to declare it once.
 *
 * @author Dmytro Dashenkov
 */
@SPI
public abstract class AbstractDatastoreColumnType<J, C> implements DatastoreColumnType<J, C> {

    /**
     * {@inheritDoc}
     *
     * <p>Delegates the call to the Datastore-native {@link BaseEntity.Builder#setNull setNull}.
     */
    @Override
    public void setNull(BaseEntity.Builder storageRecord, String columnIdentifier) {
        storageRecord.setNull(columnIdentifier);
    }

    @Override
    public void setColumnValue(BaseEntity.Builder storageRecord, C value, String columnIdentifier) {
        final Value<?> dsValue = toValue(value);
        storageRecord.set(columnIdentifier, dsValue);
    }
}
