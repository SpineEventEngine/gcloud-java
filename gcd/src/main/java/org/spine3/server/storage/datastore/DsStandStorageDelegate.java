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

package org.spine3.server.storage.datastore;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.FieldMasks;
import org.spine3.server.entity.storage.ColumnTypeRegistry;
import org.spine3.server.entity.storage.EntityRecordWithStorageFields;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.storage.datastore.type.DatastoreColumnType;
import org.spine3.type.TypeUrl;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.datastore.DsProperties.activedEntityPredicate;
import static org.spine3.validate.Validate.isDefault;

/**
 * @author Dmytro Dashenkov
 */
public class DsStandStorageDelegate extends DsRecordStorage<AggregateStateId> {

    private static final Kind KIND = Kind.of("spine3.stand_storage_record");

    private static final String TYPE_URL_KEY = "type_url";

    /**
     * Creates a new storage instance.
     *
     * @param datastore the Datastore implementation to use
     */
    public DsStandStorageDelegate(DatastoreWrapper datastore, boolean multitenant) {
        super(EntityRecord.getDescriptor(),
              datastore,
              multitenant,
              AggregateStateId.class,
              ColumnTypeRegistry.<DatastoreColumnType>empty());
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Overrides a pure method behavior
    @Override
    protected Kind getDefaultKind() {
        return KIND;
    }

    @Override
    protected Entity entityRecordToEntity(AggregateStateId id, EntityRecordWithStorageFields record) {
        final Entity incompleteEntity = super.entityRecordToEntity(id, record);
        final String typeUrl = record.getRecord()
                                     .getState()
                                     .getTypeUrl();
        final Entity.Builder builder = Entity.newBuilder(incompleteEntity)
                                             .set(TYPE_URL_KEY, typeUrl);
        final Entity completeEntity = builder.build();
        return completeEntity;
    }

    public Map<?, EntityRecord> readAllByType(final TypeUrl typeUrl, final FieldMask fieldMask) {
        return queryAllByType(typeUrl,
                              fieldMask);
    }

    public Map<?, EntityRecord> readAllByType(final TypeUrl typeUrl) {
        return queryAllByType(typeUrl,
                              FieldMask.getDefaultInstance());
    }

    protected Map<AggregateStateId, EntityRecord> queryAllByType(TypeUrl typeUrl,
                                                                 FieldMask fieldMask) {
        final EntityQuery query = buildByTypeQuery(typeUrl);

        final List<Entity> results = getDatastore().read(query);

        final Predicate<Entity> archivedAndDeletedFilter = activedEntityPredicate();

        final ImmutableMap.Builder<AggregateStateId, EntityRecord> records = new ImmutableMap.Builder<>();
        for (Entity entity : results) {
            if (!archivedAndDeletedFilter.apply(entity)) {
                continue;
            }
            final IdRecordPair<AggregateStateId> recordPair = getRecordFromEntity(entity, typeUrl);
            EntityRecord record = recordPair.getRecord();

            if (!isDefault(fieldMask)) {
                Message state = AnyPacker.unpack(record.getState());
                state = FieldMasks.applyMask(fieldMask, state, typeUrl);
                record = EntityRecord.newBuilder(record)
                                     .setState(AnyPacker.pack(state))
                                     .build();
            }
            records.put(recordPair.getId(), record);
        }

        return records.build();
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Overrides parent behavior
    @Override
    public Iterator<AggregateStateId> index() {
        checkNotClosed();

        final EntityQuery.Builder query = Query.newEntityQueryBuilder()
                                               .setKind(KIND.getValue());

        final Iterable<Entity> allEntities = getDatastore().read(query.build());
        final Iterator<AggregateStateId> idIterator =
                Iterators.transform(allEntities.iterator(),
                                    new Function<Entity, AggregateStateId>() {
                                        @Override
                                        public AggregateStateId apply(@Nullable Entity entity) {
                                            checkNotNull(entity);
                                            final AggregateStateId result = unpackKey(entity);
                                            return result;
                                        }
                                    });
        return idIterator;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
        // Ignore Storage Fields since StandStorage does not support them yet
    @Override
    protected void populateFromStorageFields(Entity.Builder entity, EntityRecordWithStorageFields record) {
        // NOP
    }

    protected EntityQuery buildByTypeQuery(TypeUrl typeUrl) {
        final EntityQuery incompleteQuery = buildAllQuery(typeUrl);
        final Filter filter = eq(TYPE_URL_KEY, typeUrl.value());
        final EntityQuery query = incompleteQuery.toBuilder()
                                                 .setFilter(filter)
                                                 .build();
        return query;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Overrides parent behavior
    @Override
    protected AggregateStateId unpackKey(Entity entity) {
        final Key key = entity.getKey();
        final String typeUrl = entity.getString(TYPE_URL_KEY);
        final TypeUrl stateType = TypeUrl.parse(typeUrl);
        final Object genericId = IdTransformer.idFromString(key.getName(), null);
        final AggregateStateId aggregateStateId = AggregateStateId.of(genericId, stateType);
        return aggregateStateId;
    }
}
