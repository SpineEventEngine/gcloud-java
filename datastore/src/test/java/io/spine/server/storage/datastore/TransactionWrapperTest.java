/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.common.truth.IntegerSubject;
import com.google.protobuf.Empty;
import io.spine.testing.SlowTest;
import io.spine.testing.server.storage.datastore.TestDatastoreWrapper;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.hasAncestor;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.localDatastore;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.generate;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`TransactionWrapper` should")
class TransactionWrapperTest {

    private static final Kind TEST_KIND = Kind.of(TypeUrl.of(Empty.class));

    private TestDatastoreWrapper datastore;
    private KeyFactory keyFactory;

    @BeforeEach
    void setUp() {
        datastore = TestDatastoreWrapper.wrap(localDatastore(), false);
        keyFactory = datastore.keyFactory(TEST_KIND);
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

    @SlowTest
    @Test
    @DisplayName("run many transactions at a time")
    void runManyAtATime() throws InterruptedException {
        int workerCount = 1117;
        ExecutorService service = newFixedThreadPool(workerCount);
        List<Key> keys = generate(() -> keyFactory.newKey(newUuid()))
                .limit(workerCount)
                .collect(toList());
        keys.stream()
            .map(key -> Entity
                    .newBuilder(key)
                    .set("a", newUuid())
                    .build())
            .forEach(entity -> service.execute(() -> {
                try (TransactionWrapper tx = datastore.newTransaction()) {
                    tx.createOrUpdate(entity);
                    tx.commit();
                }
            }));
        service.awaitTermination(5, SECONDS);
        for (Key key : keys) {
            Entity read = datastore.read(key);
            assertThat(read)
                    .isNotNull();
        }
    }

    @SlowTest
    @Test
    @DisplayName("run a single read operation for many entities")
    void bulkRead() {
        int count = 100;
        KeyFactory ancestorFactory = keyFactory
                .addAncestor(PathElement.of(TEST_KIND.value(), newUuid()));
        Entity[] entities = generate(() -> ancestorFactory.newKey(newUuid()))
                .limit(count)
                .map(key -> Entity.newBuilder(key).build())
                .toArray(Entity[]::new);
        datastore.createOrUpdate(entities);

        Key ancestorKey = ancestorFactory.newKey().getParent();
        try (TransactionWrapper tx = datastore.newTransaction()) {
            DsQueryIterator<Entity> readEntities = tx.read(Query.newEntityQueryBuilder()
                                                                .setKind(TEST_KIND.value())
                                                                .setFilter(hasAncestor(ancestorKey))
                                                                .build());
            List<Entity> allEntities = newArrayList(readEntities);
            IntegerSubject assertSize = assertThat(allEntities.size());
            assertSize.isAtLeast(10);
            assertSize.isAtMost(count);
            tx.commit();
        }
    }

    @Test
    @DisplayName("fail on `create` if entity already exists")
    void insert() {
        Key key = keyFactory.newKey(newUuid());
        Entity entity = Entity
                .newBuilder(key)
                .build();
        datastore.createOrUpdate(entity);
        try (TransactionWrapper tx = datastore.newTransaction()) {
            DatastoreException exception = assertThrows(DatastoreException.class,
                                                        () -> tx.create(entity));
            assertThat(exception)
                    .hasMessageThat()
                    .ignoringCase()
                    .contains("duplicate");
            tx.commit();
        }
    }
}
