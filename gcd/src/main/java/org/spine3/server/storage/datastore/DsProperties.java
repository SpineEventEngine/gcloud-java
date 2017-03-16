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
import org.spine3.base.Event;
import org.spine3.base.EventContextOrBuilder;
import org.spine3.base.Identifiers;
import org.spine3.protobuf.Messages;
import org.spine3.server.storage.EntityField;
import org.spine3.server.storage.LifecycleFlagField;

import javax.annotation.Nullable;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.protobuf.Timestamps2.convertToDate;
import static org.spine3.server.aggregate.storage.AggregateField.aggregate_id;
import static org.spine3.server.event.storage.EventContextField.context_event_id;
import static org.spine3.server.event.storage.EventContextField.context_of_command;
import static org.spine3.server.event.storage.EventContextField.context_timestamp;
import static org.spine3.server.event.storage.EventContextField.context_version;
import static org.spine3.server.event.storage.EventField.event_id;
import static org.spine3.server.event.storage.EventField.event_type;
import static org.spine3.server.event.storage.EventField.producer_id;
import static org.spine3.server.storage.EntityField.timestamp_nanos;
import static org.spine3.server.storage.LifecycleFlagField.archived;

/**
 * Utility class, which simplifies creation of the Datastore properties.
 *
 * @author Mikhail Mikhaylov
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
class DsProperties {

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

    private DsProperties() {
    }

    /**
     * Makes a property from the given timestamp using
     * {@link org.spine3.protobuf.Timestamps2#convertToDate(TimestampOrBuilder)}.
     */
    static void addTimestampProperty(TimestampOrBuilder timestamp, Entity.Builder entity) {
        final Date date = convertToDate(timestamp);
        entity.set(EntityField.timestamp.toString(), DateTime.copyFrom(date));
    }

    /**
     * Makes a property from the given timestamp using
     * {@link org.spine3.protobuf.Timestamps2#convertToDate(TimestampOrBuilder)}.
     */
    static void addTimestampNanosProperty(TimestampOrBuilder timestamp, Entity.Builder entity) {
        final long nanos = convertToDate(timestamp).getTime();
        entity.set(timestamp_nanos.toString(), nanos);
    }

    /**
     * Makes AggregateId property from given {@link Message} value.
     */
    static void addAggregateIdProperty(Object aggregateId, Entity.Builder entity) {
        final String propertyValue = Identifiers.idToString(aggregateId);
        entity.set(aggregate_id.toString(), propertyValue);
    }

    /**
     * Makes EventType property from given String value.
     */
    static void addEventTypeProperty(String eventType, Entity.Builder entity) {
        entity.set(event_type.toString(), eventType);
    }

    static void addArchivedProperty(Entity.Builder entity, boolean archived) {
        entity.set(LifecycleFlagField.archived.toString(), archived);
    }

    static void addDeletedProperty(Entity.Builder entity, boolean deleted) {
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
        builder.set(context_event_id.toString(), Messages.toText(context.getEventId()));
        builder.set(context_timestamp.toString(), convertToDate(context.getTimestamp()).getTime());
        // We do not re-save producer id
        builder.set(context_of_command.toString(), Messages.toText(context.getCommandContext()));
        builder.set(context_version.toString(), context.getVersion()
                                                       .getNumber());
    }

    /**
     * Converts {@link org.spine3.base.Event}'s fields to a set of Properties, which are
     * ready to add to the Datastore entity.
     */
    static void makeEventFieldProperties(Event event,
                                         Entity.Builder builder) {
        // We do not re-save timestamp
        // We do not re-save event type
        final String eventId = Identifiers.idToString(
                event.getContext()
                     .getEventId());
        final String producerId = Identifiers.idToString(
                event.getContext()
                     .getProducerId());
        builder.set(event_id.toString(), eventId);
        builder.set(producer_id.toString(), producerId);
    }

    static Predicate<Entity> activeEntityPredicate() {
        return NOT_ARCHIVED_OR_DELETED;
    }
}
