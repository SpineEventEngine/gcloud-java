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
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;
import io.spine.server.storage.LifecycleFlagField;
import io.spine.server.storage.StorageField;
import io.spine.string.Stringifiers;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.aggregate.AggregateField.aggregate_id;
import static io.spine.server.storage.LifecycleFlagField.archived;

/**
 * Utility class, which simplifies creation of the Datastore properties.
 *
 * @author Mikhail Mikhaylov
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
final class DsProperties {

    private DsProperties() {
        // Prevent utility class instantiation.
    }

    /**
     * Makes AggregateId property from given {@link Message} value.
     */
    static void addAggregateId(Entity.Builder entity, Object aggregateId) {
        String propertyValue = Stringifiers.toString(aggregateId);
        entity.set(aggregate_id.toString(), propertyValue);
    }

    static void addWhenCreated(Entity.Builder entity, Timestamp when) {
        com.google.cloud.Timestamp value = ofTimeSecondsAndNanos(when.getSeconds(),
                                                                       when.getNanos());
        AggregateEventRecordProperty.created.setProperty(entity, value);
    }

    static void addVersion(Entity.Builder entity, Version version) {
        int number = version.getNumber();
        AggregateEventRecordProperty.version.setProperty(entity, number);
    }

    static void markAsSnapshot(Entity.Builder entity, boolean snapshot) {
        AggregateEventRecordProperty.snapshot.setProperty(entity, snapshot);
    }

    static void markAsArchived(Entity.Builder entity, boolean archived) {
        entity.set(LifecycleFlagField.archived.toString(), archived);
    }

    static void markAsDeleted(Entity.Builder entity, boolean deleted) {
        entity.set(LifecycleFlagField.deleted.toString(), deleted);
    }

    static boolean isArchived(Entity entity) {
        checkNotNull(entity);
        return hasFlag(entity, archived.toString());
    }

    static boolean isDeleted(Entity entity) {
        checkNotNull(entity);
        return hasFlag(entity, LifecycleFlagField.deleted.toString());
    }

    static OrderBy byCreatedTime() {
        return AggregateEventRecordOrdering.BY_CREATED.getOrdering();
    }

    static OrderBy byVersion() {
        return AggregateEventRecordOrdering.BY_VERSION.getOrdering();
    }

    static OrderBy byRecordType() {
        return AggregateEventRecordOrdering.BY_SNAPSHOT.getOrdering();
    }

    private static boolean hasFlag(Entity entity, String flagName) {
        boolean result = entity.contains(flagName)
                            && entity.getBoolean(flagName);
        return result;
    }

    private static OrderBy asc(StorageField property) {
        return OrderBy.asc(property.toString());
    }

    private static OrderBy desc(StorageField property) {
        return OrderBy.desc(property.toString());
    }

    private enum AggregateEventRecordProperty implements StorageField {

        /**
         * A property storing the Event creation time.
         */
        created {
            @Override
            void setProperty(Entity.Builder builder, Object value) {
                com.google.cloud.Timestamp dateTime = (com.google.cloud.Timestamp) value;
                builder.set(toString(), dateTime);
            }
        },

        /**
         * A property storing the Aggregate version.
         */
        version {
            @Override
            void setProperty(Entity.Builder builder, Object value) {
                int version = (int) value;
                builder.set(toString(), version);
            }
        },

        /**
         * A boolean property storing {@code true} if the Record represents a Snapshot and
         * {@code false} otherwise.
         */
        snapshot {
            @Override
            void setProperty(Entity.Builder builder, Object value) {
                boolean isSnapshot = (boolean) value;
                builder.set(toString(), isSnapshot);
            }
        };

        abstract void setProperty(Entity.Builder builder, Object value);

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    private enum AggregateEventRecordOrdering {

        BY_CREATED(desc(AggregateEventRecordProperty.created)),
        BY_VERSION(desc(AggregateEventRecordProperty.version)),
        BY_SNAPSHOT(asc(AggregateEventRecordProperty.snapshot));

        private final OrderBy ordering;

        AggregateEventRecordOrdering(OrderBy ordering) {
            this.ordering = ordering;
        }

        private OrderBy getOrdering() {
            return ordering;
        }
    }
}
