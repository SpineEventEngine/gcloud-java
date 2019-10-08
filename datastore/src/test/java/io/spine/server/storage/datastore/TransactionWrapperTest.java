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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.protobuf.Empty;
import io.spine.testing.server.storage.datastore.TestDatastoreWrapper;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.localDatastore;
import static io.spine.server.storage.datastore.tenant.TestNamespaceSuppliers.singleTenant;

@DisplayName("`TransactionWrapper` should")
class TransactionWrapperTest {

    private TestDatastoreWrapper datastore;
    private KeyFactory keyFactory;

    @BeforeEach
    void setUp() {
        datastore = TestDatastoreWrapper.wrap(localDatastore(), false);
        keyFactory = datastore.keyFactory(Kind.of(TypeUrl.of(Empty.class).value()));
    }

    @AfterEach
    void cleanUpDatastore() {
        datastore.dropAllTables();
    }

    @Test
    @DisplayName("write transactionally")
    void write() {
        Key key = keyFactory.newKey(newUuid());
        Entity entity = Entity.newBuilder(key)
                              .set("field", 42)
                              .build();
        TransactionWrapper tx = datastore.newTransaction();
        tx.createOrUpdate(entity);
        tx.commit();
        Entity readEntity = datastore.read(key);
        assertThat(readEntity)
                .isEqualTo(entity);
    }

    @Test
    @DisplayName("read transactionally")
    void read() {
        Key key = keyFactory.newKey(newUuid());
        Entity entity = Entity.newBuilder(key)
                              .set("field_name", newUuid())
                              .build();
        datastore.create(entity);
        TransactionWrapper tx = datastore.newTransaction();
        Optional<Entity> read = tx.read(key);
        tx.commit();
        Entity readEntity = read.orElseGet(Assertions::fail);
        assertThat(readEntity)
                .isEqualTo(entity);
    }

    @Test
    @DisplayName("read and write transactionally")
    void readWrite() {
        Key key = keyFactory.newKey(newUuid());
        String field = "number";
        Entity original = Entity.newBuilder(key)
                                .set(field, 1)
                                .build();
        datastore.create(original);
        TransactionWrapper tx = datastore.newTransaction();
        Entity updated = Entity.newBuilder(original)
                               .set(field, 2)
                               .build();
        assertThat(tx.read(key)
                     .orElseGet(Assertions::fail)
                     .getLong(field))
                .isEqualTo(original.getLong(field));

        tx.createOrUpdate(updated);

        assertThat(tx.read(key)
                     .orElseGet(Assertions::fail)
                     .getLong(field))
                .isEqualTo(original.getLong(field));

        tx.commit();

        Entity finalEntity = datastore.read(key);
        assertThat(finalEntity)
                .isNotNull();
        assertThat(finalEntity.getLong(field))
                .isEqualTo(updated.getLong(field));
    }

    @Test
    @DisplayName("rollback changes")
    void rollback() {
        Key key = keyFactory.newKey(newUuid());
        Entity entity = Entity.newBuilder(key)
                              .set("example", newUuid())
                              .build();
        TransactionWrapper tx = datastore.newTransaction();
        tx.createOrUpdate(entity);
        tx.rollback();
        Entity readEntity = datastore.read(key);
        assertThat(readEntity)
                .isNull();
    }

    @Test
    @DisplayName("rollback on close")
    void closeAndRollback() {
        Key key = keyFactory.newKey(newUuid());
        try (TransactionWrapper tx = datastore.newTransaction()) {
            Entity entity = Entity.newBuilder(key)
                                  .set("test", newUuid())
                                  .build();
            tx.createOrUpdate(entity);
        }
        Entity readEntity = datastore.read(key);
        assertThat(readEntity)
                .isNull();
    }

    @Test
    @DisplayName("not rollback if already committed")
    void notRollbackIfNotActive() {
        Key key = keyFactory.newKey(newUuid());
        try (TransactionWrapper tx = datastore.newTransaction()) {
            Entity entity = Entity.newBuilder(key)
                                  .set("test1", newUuid())
                                  .build();
            tx.createOrUpdate(entity);
            tx.commit();
        }
        Entity readEntity = datastore.read(key);
        assertThat(readEntity)
                .isNotNull();
    }
}
