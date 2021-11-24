/*
 * Copyright 2021, TeamDev. All rights reserved.
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

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
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
import java.util.concurrent.Callable;

import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.hasAncestor;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.datastore.given.DatastoreWrapperTestEnv.localDatastore;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.generate;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`TransactionWrapper` should")
@SuppressWarnings("ClassWithTooManyMethods")    /* It's fine for a test. */
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
        var key = keyFactory.newKey(newUuid());
        var entity = Entity.newBuilder(key)
                           .set("field", 42)
                           .build();
        var tx = datastore.newTransaction();
        tx.createOrUpdate(entity);
        tx.commit();

        var readEntity = assertEntityExists(key);
        assertThat(readEntity)
                .isEqualTo(entity);
    }

    @Test
    @DisplayName("read transactionally")
    void read() {
        var key = keyFactory.newKey(newUuid());
        var entity = Entity.newBuilder(key)
                           .set("field_name", newUuid())
                           .build();
        datastore.create(entity);
        var tx = datastore.newTransaction();
        var read = tx.read(key);
        tx.commit();
        var readEntity = read.orElseGet(Assertions::fail);
        assertThat(readEntity)
                .isEqualTo(entity);
    }

    @Test
    @DisplayName("read and write transactionally")
    void readWrite() {
        var key = keyFactory.newKey(newUuid());
        var field = "number";
        var original = Entity.newBuilder(key)
                             .set(field, 1)
                             .build();
        datastore.create(original);
        var tx = datastore.newTransaction();
        var updated = Entity.newBuilder(original)
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

        var finalEntity = datastore.read(key);
        assertThat(finalEntity).isPresent();
        assertThat(finalEntity.get()
                              .getLong(field))
                .isEqualTo(updated.getLong(field));
    }

    @Test
    @DisplayName("delete single entity and many entities in a bulk by their keys transactionally")
    void writeDelete() {
        var entityCount = 20;
        var entities =
                generate(() -> Entity.newBuilder(keyFactory.newKey(newUuid()))
                                     .set("uuid", newUuid())
                                     .build())
                        .limit(entityCount)
                        .collect(toList());

        var creationTx = datastore.newTransaction();
        creationTx.create(entities);
        creationTx.commit();

        var firstEntity = entities.remove(0);
        var singleDeletionTx = datastore.newTransaction();
        var toDelete = firstEntity.getKey();
        singleDeletionTx.delete(toDelete);
        singleDeletionTx.commit();

        var firstReadTx = datastore.newTransaction();
        assertThat(firstReadTx.read(toDelete))
                .isEmpty();
        firstReadTx.commit();

        var bulkDeletionTx = datastore.newTransaction();
        var remainingKeys = entities.stream()
                                    .map(Entity::getKey)
                                    .collect(toList());
        var keysToDelete = Iterables.toArray(remainingKeys, Key.class);
        bulkDeletionTx.delete(keysToDelete);
        bulkDeletionTx.commit();

        var bulkReadTx = datastore.newTransaction();
        for (var key : keysToDelete) {
            assertNoEntityWith(key);
        }
        bulkReadTx.commit();
    }

    @Test
    @DisplayName("rollback changes")
    void rollback() {
        var key = keyFactory.newKey(newUuid());
        var entity = Entity.newBuilder(key)
                           .set("example", newUuid())
                           .build();
        var tx = datastore.newTransaction();
        tx.createOrUpdate(entity);
        tx.rollback();

        assertNoEntityWith(key);
    }

    @Test
    @DisplayName("rollback on close")
    void closeAndRollback() {
        var key = keyFactory.newKey(newUuid());
        try (var tx = datastore.newTransaction()) {
            var entity = Entity.newBuilder(key)
                               .set("test", newUuid())
                               .build();
            tx.createOrUpdate(entity);
        }
        assertNoEntityWith(key);
    }

    @Test
    @DisplayName("not rollback if already committed")
    void notRollbackIfNotActive() {
        var key = keyFactory.newKey(newUuid());
        try (var tx = datastore.newTransaction()) {
            var entity = Entity.newBuilder(key)
                               .set("test1", newUuid())
                               .build();
            tx.createOrUpdate(entity);
            tx.commit();
        }
        assertEntityExists(key);
    }

    @SlowTest
    @Test
    @DisplayName("run many transactions at a time")
    void runManyAtATime() throws InterruptedException {
        var workerCount = 117;
        var service = newFixedThreadPool(workerCount);
        var keys = generate(() -> keyFactory.newKey(newUuid()))
                .limit(workerCount)
                .collect(toList());
        var tasks =
                keys.stream()
                    .map(key -> Entity
                            .newBuilder(key)
                            .set("a", newUuid())
                            .build())
                    .map(this::asEntityWriteJob)
                    .collect(toList());
        service.invokeAll(tasks);
        assertThat(service.shutdownNow()).isEmpty();
        for (var key : keys) {
            assertEntityExists(key);
        }
    }

    /**
     * Returns a {@code Callable} writing the passed entity to the storage in a new transaction.
     *
     * <p>If the entity already exists, it will be overwritten. If there is no such entity in
     * the storage, the entity record will be created.
     *
     * <p>The result of the created {@code Callable} is the {@link Key} assigned to the entity.
     */
    private Callable<Key> asEntityWriteJob(Entity entity) {
        return () -> {
            try (var tx = datastore.newTransaction()) {
                tx.createOrUpdate(entity);
                tx.commit();
                var key = entity.getKey();
                assertEntityExists(key);
            }
            return entity.getKey();
        };
    }

    @SlowTest
    @Test
    @DisplayName("run a single read operation for many entities")
    void bulkRead() {
        var count = 100;
        var ancestorFactory = keyFactory
                .addAncestor(PathElement.of(TEST_KIND.value(), newUuid()));
        var entities =
                generate(() -> ancestorFactory.newKey(newUuid()))
                        .limit(count)
                        .map(key -> Entity.newBuilder(key)
                                          .build())
                        .collect(toImmutableList());
        datastore.createOrUpdate(entities);

        var ancestorKey = ancestorFactory.newKey()
                                         .getParent();
        try (var tx = datastore.newTransaction()) {
            var readEntities = tx.read(Query.newEntityQueryBuilder()
                                            .setKind(TEST_KIND.value())
                                            .setFilter(hasAncestor(ancestorKey))
                                            .build());
            List<Entity> allEntities = newArrayList(readEntities);
            assertThat(allEntities.size()).isEqualTo(count);
            tx.commit();
        }
    }

    @Test
    @DisplayName("fail early on `create` if entity already exists")
    void insertTwice() {
        var key = keyFactory.newKey(newUuid());
        var entity = Entity.newBuilder(key)
                           .build();
        try (var tx = datastore.newTransaction()) {
            tx.createOrUpdate(entity);
            var exception = assertThrows(DatastoreException.class,
                                         () -> tx.create(entity));
            assertThat(exception)
                    .hasMessageThat()
                    .ignoringCase()
                    .contains("already added");
            tx.commit();
        }
    }

    @Test
    @DisplayName("fail on `create` if entity existed before transaction")
    void insert() {
        var key = keyFactory.newKey(newUuid());
        var propertyName = "randomValue";
        var oldEntity = Entity.newBuilder(key)
                              .build();
        var newEntity = Entity.newBuilder(key)
                              .set(propertyName, 42L)
                              .build();
        datastore.createOrUpdate(oldEntity);
        try (var tx = datastore.newTransaction()) {
            tx.create(newEntity);
            var exception = assertThrows(DatastoreException.class, tx::commit);
            assertThat(exception)
                    .hasMessageThat()
                    .ignoringCase()
                    .contains("already exists");
        }
    }

    @Test
    @DisplayName("fail on `create` if one of entities existed before transaction")
    void insertMany() {
        var key = keyFactory.newKey(newUuid());
        var propertyName = "some_property";
        var oldEntity = Entity.newBuilder(key)
                              .build();
        var newEntity = Entity.newBuilder(key)
                              .set(propertyName, 42L)
                              .build();
        datastore.createOrUpdate(oldEntity);
        var freshNewKey = keyFactory.newKey(newUuid());
        try (var tx = datastore.newTransaction()) {
            var freshNewEntity = Entity.newBuilder(freshNewKey)
                                       .build();
            tx.create(ImmutableList.of(freshNewEntity, newEntity));
            assertThrows(DatastoreException.class, tx::commit);
        }
        assertThat(datastore.read(freshNewKey))
                .isEmpty();
        var oldEntityRead = datastore.read(oldEntity.getKey());
        assertThat(oldEntityRead)
                .isPresent();
        assertThat(oldEntityRead.get())
                .isEqualTo(oldEntity);
    }

    @Test
    @DisplayName("put many entities into database")
    void putMany() {
        var propertyName = "foo";
        var key = keyFactory.newKey(newUuid());
        var oldEntity = Entity.newBuilder(key)
                              .build();
        var newEntity = Entity.newBuilder(key)
                              .set(propertyName, 42L)
                              .build();
        datastore.createOrUpdate(oldEntity);
        var freshNewKey = keyFactory.newKey(newUuid());
        try (var tx = datastore.newTransaction()) {
            var freshNewEntity = Entity.newBuilder(freshNewKey)
                                       .build();
            tx.createOrUpdate(ImmutableList.of(freshNewEntity, newEntity));
            tx.commit();
        }
        assertThat(datastore.read(freshNewKey))
                .isPresent();
        var oldEntityRead = datastore.read(oldEntity.getKey());
        assertThat(oldEntityRead)
                .isPresent();
        assertThat(oldEntityRead.get())
                .isEqualTo(newEntity);
    }

    @Test
    @DisplayName("read multiple entities by IDs")
    void lookup() {
        var count = 10;
        var entities =
                generate(() -> keyFactory.newKey(newUuid()))
                        .limit(count)
                        .map(key -> Entity.newBuilder(key)
                                          .build())
                        .collect(toImmutableList());
        datastore.createOrUpdate(entities);

        var firstEntity = entities.get(2);
        var secondEntity = entities.get(5);
        List<Key> keys = ImmutableList.of(
                firstEntity.getKey(),
                keyFactory.newKey(newUuid()),
                secondEntity.getKey()
        );
        try (var tx = datastore.newTransaction()) {
            var readEntities = tx.lookup(keys);
            tx.commit();
            assertThat(readEntities)
                    .containsExactly(firstEntity, null, secondEntity);
        }
    }

    @SlowTest
    @Test
    @DisplayName("read many entities by IDs")
    void lookupBulk() {
        var count = 2020;
        keyFactory.addAncestor(PathElement.of(TEST_KIND.value(), newUuid()));
        var entities =
                generate(() -> keyFactory.newKey(newUuid()))
                        .limit(count)
                        .map(key -> Entity.newBuilder(key)
                                          .build())
                        .collect(toImmutableList());
        datastore.createOrUpdate(entities);

        var keys = entities.stream()
                           .map(BaseEntity::getKey)
                           .collect(toList());
        try (var tx = datastore.newTransaction()) {
            var readEntities = tx.lookup(keys);
            tx.commit();
            assertThat(readEntities)
                    .containsExactlyElementsIn(entities);
        }
    }

    @Test
    @DisplayName("NOT execute a key query")
    void keyQuery() {
        var count = 2;
        var entities = generate(() -> keyFactory.newKey(newUuid()))
                .limit(count)
                .map(key -> Entity.newBuilder(key)
                                  .build())
                .collect(toList());
        datastore.createOrUpdate(entities);
        try (var tx = datastore.newTransaction()) {
            StructuredQuery<Key> query = Query
                    .newKeyQueryBuilder()
                    .setKind(TEST_KIND.value())
                    .build();
            assertThrows(DatastoreException.class, () -> tx.read(query));
            tx.commit();
        }
    }

    @CanIgnoreReturnValue
    private Entity assertEntityExists(Key key) {
        var result = datastore.read(key);
        assertThat(result)
                .isPresent();
        return result.get();
    }

    private void assertNoEntityWith(Key key) {
        var readEntity = datastore.read(key);
        assertThat(readEntity)
                .isEmpty();
    }
}
