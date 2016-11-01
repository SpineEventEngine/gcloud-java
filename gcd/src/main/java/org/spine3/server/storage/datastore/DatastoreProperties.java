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

import com.google.cloud.datastore.DateTime;
import com.google.cloud.datastore.Entity;
import com.google.protobuf.Message;
import com.google.protobuf.TimestampOrBuilder;
import org.spine3.base.EventContextOrBuilder;
import org.spine3.base.Identifiers;
import org.spine3.protobuf.Messages;
import org.spine3.server.storage.EventStorageRecordOrBuilder;

import java.util.Date;

import static org.spine3.protobuf.Timestamps.convertToDate;
import static org.spine3.protobuf.Timestamps.convertToNanos;

/**
 * Utility class, which simplifies creation of datastore properties.
 *
 * @author Mikhail Mikhaylov
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
/* package */ class DatastoreProperties {

    /* package */ static final String TIMESTAMP_PROPERTY_NAME = "timestamp";
    /* package */ static final String TIMESTAMP_NANOS_PROPERTY_NAME = "timestamp_nanos";

    /* package */ static final String AGGREGATE_ID_PROPERTY_NAME = "aggregate_id";
    private static final String PRODUCER_ID_PROPERTY_NAME = "producer_id";
    /* package */ static final String EVENT_TYPE_PROPERTY_NAME = "event_type";
    private static final String EVENT_ID_PROPERTY_NAME = "event_id";

    private static final String CONTEXT_FIELD_PROPERTY_PREFIX_NAME = "context_";

    private static final String CONTEXT_EVENT_ID_PROPERTY_NAME = "context_event_id";
    private static final String CONTEXT_TIMESTAMP_PROPERTY_NAME = "context_timestamp";
    private static final String CONTEXT_OF_COMMAND_PROPERTY_NAME = "context_of_command";
    private static final String CONTEXT_VERSION = "context_version";

    private DatastoreProperties() {
    }

    /**
     * Makes a property from the given timestamp using
     * {@link org.spine3.protobuf.Timestamps#convertToDate(TimestampOrBuilder)}.
     */
    /* package */
    static void addTimestampProperty(TimestampOrBuilder timestamp, Entity.Builder entity) {
        final Date date = convertToDate(timestamp);
        entity.set(TIMESTAMP_PROPERTY_NAME, DateTime.copyFrom(date));
    }

    /**
     * Makes a property from the given timestamp using
     * {@link org.spine3.protobuf.Timestamps#convertToNanos(TimestampOrBuilder)}.
     */
    /* package */ static void addTimestampNanosProperty(TimestampOrBuilder timestamp, Entity.Builder entity) {
        final long nanos = convertToNanos(timestamp);
        entity.set(TIMESTAMP_NANOS_PROPERTY_NAME, nanos);
    }

    /**
     * Makes AggregateId property from given {@link Message} value.
     */
    /* package */ static void addAggregateIdProperty(Object aggregateId, Entity.Builder entity) {
        final String propertyValue = Identifiers.idToString(aggregateId);
        entity.set(AGGREGATE_ID_PROPERTY_NAME, propertyValue);
    }

    /**
     * Makes EventType property from given String value.
     */
    /* package */ static void addEventTypeProperty(String eventType, Entity.Builder entity) {
        entity.set(EVENT_TYPE_PROPERTY_NAME, eventType);
    }

    private static String getContextFieldPropertyName(String contextFieldName) {
        return CONTEXT_FIELD_PROPERTY_PREFIX_NAME + contextFieldName;
    }

    /**
     * Converts {@link org.spine3.base.EventContext} or it's builder to a set of Properties, which are
     * ready to add to datastore entity.
     */
    /* package */ static void makeEventContextProperties(EventContextOrBuilder context,
                                           Entity.Builder builder) {
        builder.set(CONTEXT_EVENT_ID_PROPERTY_NAME, Messages.toText(context.getEventId()));
        builder.set(CONTEXT_TIMESTAMP_PROPERTY_NAME, convertToNanos(context.getTimestamp()));
        // We do not re-save producer id
        builder.set(CONTEXT_OF_COMMAND_PROPERTY_NAME, Messages.toText(context.getCommandContext()));
        builder.set(CONTEXT_VERSION, context.getVersion());
    }

    /**
     * Converts {@link org.spine3.base.Event}'s fields to a set of Properties, which are
     * ready to add to datastore entity.
     */
    /* package */ static void makeEventFieldProperties(EventStorageRecordOrBuilder event,
                                         Entity.Builder builder) {
        // We do not re-save timestamp
        // We do not re-save event type
        builder.set(EVENT_ID_PROPERTY_NAME, event.getEventId());
        builder.set(PRODUCER_ID_PROPERTY_NAME, event.getProducerId());
    }
}
