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

import com.google.cloud.datastore.DateTime;
import com.google.cloud.datastore.Entity;
import com.google.common.base.Predicate;
import com.google.protobuf.Message;
import com.google.protobuf.TimestampOrBuilder;
import org.spine3.base.EventContextOrBuilder;
import org.spine3.base.Stringifiers;
import org.spine3.protobuf.Messages;
import org.spine3.server.event.storage.EventStorageRecord;

import javax.annotation.Nullable;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.protobuf.Timestamps.convertToDate;
import static org.spine3.protobuf.Timestamps.convertToNanos;
import static org.spine3.server.storage.datastore.EntityFields.AggregateFields.AGGREGATE_ID;
import static org.spine3.server.storage.datastore.EntityFields.CommonFields.TIMESTAMP;
import static org.spine3.server.storage.datastore.EntityFields.CommonFields.TIMESTAMP_NANOS;
import static org.spine3.server.storage.datastore.EntityFields.EntityStateFields.ARCHIVED;
import static org.spine3.server.storage.datastore.EntityFields.EntityStateFields.DELETED;
import static org.spine3.server.storage.datastore.EntityFields.EventContextFields.CONTEXT_EVENT_ID;
import static org.spine3.server.storage.datastore.EntityFields.EventContextFields.CONTEXT_OF_COMMAND;
import static org.spine3.server.storage.datastore.EntityFields.EventContextFields.CONTEXT_TIMESTAMP;
import static org.spine3.server.storage.datastore.EntityFields.EventContextFields.CONTEXT_VERSION;
import static org.spine3.server.storage.datastore.EntityFields.EventFields.EVENT_ID;
import static org.spine3.server.storage.datastore.EntityFields.EventFields.EVENT_TYPE;
import static org.spine3.server.storage.datastore.EntityFields.EventFields.PRODUCER_ID;

/**
 * Utility class, which simplifies creation of the Datastore properties.
 *
 * @author Mikhail Mikhaylov
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
class DatastoreProperties {

    private static final Predicate<Entity> NOT_ARCHIVED_OR_DELETED = new Predicate<Entity>() {
        @Override
        public boolean apply(@Nullable Entity input) {
            if (input == null) {
                return false;
            }
            final boolean isNotArchived = !isArchived(input);
            final boolean isNotDeleted = !isDeleted(input);
            return isNotArchived && isNotDeleted;
        }
    };

    private DatastoreProperties() {
    }

    /**
     * Makes a property from the given timestamp using
     * {@link org.spine3.protobuf.Timestamps#convertToDate(TimestampOrBuilder)}.
     */
    static void addTimestampProperty(TimestampOrBuilder timestamp, Entity.Builder entity) {
        final Date date = convertToDate(timestamp);
        entity.set(TIMESTAMP.toString(), DateTime.copyFrom(date));
    }

    /**
     * Makes a property from the given timestamp using
     * {@link org.spine3.protobuf.Timestamps#convertToNanos(TimestampOrBuilder)}.
     */
    static void addTimestampNanosProperty(TimestampOrBuilder timestamp, Entity.Builder entity) {
        final long nanos = convertToNanos(timestamp);
        entity.set(TIMESTAMP_NANOS.toString(), nanos);
    }

    /**
     * Makes AggregateId property from given {@link Message} value.
     */
    static void addAggregateIdProperty(Object aggregateId, Entity.Builder entity) {
        final String propertyValue = Stringifiers.idToString(aggregateId);
        entity.set(AGGREGATE_ID.toString(), propertyValue);
    }

    /**
     * Makes EventType property from given String value.
     */
    static void addEventTypeProperty(String eventType, Entity.Builder entity) {
        entity.set(EVENT_TYPE.toString(), eventType);
    }

    static void addArchivedProperty(Entity.Builder entity, boolean archived) {
        entity.set(ARCHIVED.toString(), archived);
    }

    static void addDeletedProperty(Entity.Builder entity, boolean deleted) {
        entity.set(DELETED.toString(), deleted);
    }

    static boolean isArchived(Entity entity) {
        checkNotNull(entity);
        return hasFlag(entity, ARCHIVED.toString());
    }

    static boolean isDeleted(Entity entity) {
        checkNotNull(entity);
        return hasFlag(entity, DELETED.toString());
    }

    private static boolean hasFlag(Entity entity, String flagName) {
        final boolean result =
                entity.contains(flagName)
                        && entity.getBoolean(flagName);
        return result;
    }

    /**
     * Converts {@link org.spine3.base.EventContext} or it's builder to a set of Properties, which are
     * ready to add to the Datastore entity.
     */
    static void makeEventContextProperties(EventContextOrBuilder context,
                                           Entity.Builder builder) {
        builder.set(CONTEXT_EVENT_ID.toString(), Messages.toText(context.getEventId()));
        builder.set(CONTEXT_TIMESTAMP.toString(), convertToNanos(context.getTimestamp()));
        // We do not re-save producer id
        builder.set(CONTEXT_OF_COMMAND.toString(), Messages.toText(context.getCommandContext()));
        builder.set(CONTEXT_VERSION.toString(), context.getVersion());
    }

    /**
     * Converts {@link org.spine3.base.Event}'s fields to a set of Properties, which are
     * ready to add to the Datastore entity.
     */
    static void makeEventFieldProperties(EventStorageRecord event,
                                         Entity.Builder builder) {
        // We do not re-save timestamp
        // We do not re-save event type
        builder.set(EVENT_ID.toString(), event.getEventId());
        builder.set(PRODUCER_ID.toString(), event.getProducerId());
    }

    static Predicate<Entity> activeEntityPredicate() {
        return NOT_ARCHIVED_OR_DELETED;
    }
}
