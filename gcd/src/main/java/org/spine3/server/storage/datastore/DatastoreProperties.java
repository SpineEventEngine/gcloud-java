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

import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.PropertyReference;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.google.protobuf.TimestampOrBuilder;
import org.spine3.base.EventContextOrBuilder;
import org.spine3.protobuf.Messages;
import org.spine3.server.storage.EventStorageRecordOrBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import static com.google.api.services.datastore.client.DatastoreHelper.makeProperty;
import static com.google.api.services.datastore.client.DatastoreHelper.makePropertyReference;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;
import static org.spine3.protobuf.Timestamps.convertToDate;
import static org.spine3.protobuf.Timestamps.convertToNanos;

/**
 * Utility class, which simplifies creation of datastore properties.
 *
 * @author Mikhail Mikhaylov
 */
@SuppressWarnings("UtilityClass")
/* package */ class DatastoreProperties {

    private static final String TIMESTAMP_PROPERTY_NAME = "timestamp";
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
    /* package */ static Property.Builder makeTimestampProperty(TimestampOrBuilder timestamp) {
        final Date date = convertToDate(timestamp);
        return makeProperty(TIMESTAMP_PROPERTY_NAME, makeValue(date));
    }

    /**
     * Makes a property from the given timestamp using
     * {@link org.spine3.protobuf.Timestamps#convertToNanos(TimestampOrBuilder)}.
     */
    /* package */ static Property.Builder makeTimestampNanosProperty(TimestampOrBuilder timestamp) {
        final long nanos = convertToNanos(timestamp);
        return makeProperty(TIMESTAMP_NANOS_PROPERTY_NAME, makeValue(nanos));
    }

    /**
     * Makes AggregateId property from given {@link Message} value.
     *
     * @return {@link Property.Builder}
     */
    /* package */ static Property.Builder makeAggregateIdProperty(Message aggregateId) {
        final String propertyValue = Messages.toText(aggregateId);
        return makeProperty(AGGREGATE_ID_PROPERTY_NAME, makeValue(propertyValue));
    }

    /**
     * Makes EventType property from given String value.
     *
     * @return {@link Property.Builder}
     */
    /* package */ static Property.Builder makeEventTypeProperty(String eventType) {
        return makeProperty(EVENT_TYPE_PROPERTY_NAME, makeValue(eventType));
    }

    /**
     * Makes Property, based on context {@link FieldMask}.
     *
     * @return {@link Property.Builder}
     */
    @SuppressWarnings("TypeMayBeWeakened") // No need to do this
    /* package */ static PropertyReference makeContextFieldPropertyReference(FieldMask field) {
        final String fieldPath = field.getPaths(0);
        final String propertyName = getContextFieldPropertyName(fieldPath);

        return makePropertyReference(propertyName).build();
    }

    /**
     * Makes Property, based on event's {@link FieldMask}.
     *
     * @return {@link Property.Builder}
     */
    @SuppressWarnings("TypeMayBeWeakened") // No need to do this
    /* package */ static PropertyReference makeEventFieldPropertyReference(FieldMask field) {
        final String fieldPath = field.getPaths(0);

        return makePropertyReference(fieldPath).build();
    }

    private static String getContextFieldPropertyName(String contextFieldName) {
        return CONTEXT_FIELD_PROPERTY_PREFIX_NAME + contextFieldName;
    }

    /**
     * Converts {@link org.spine3.base.EventContext} or it's builder to a set of Properties, which are
     * ready to add to datastore entity.
     */
    /* package */ static Iterable<? extends Property> makeEventContextProperties(
            EventContextOrBuilder context) {
        final Collection<Property> properties = new ArrayList<>();
        properties.add(makeProperty(CONTEXT_EVENT_ID_PROPERTY_NAME,
                makeValue(Messages.toText(context.getEventId()))).build());
        properties.add(makeProperty(CONTEXT_TIMESTAMP_PROPERTY_NAME,
                makeValue(convertToNanos(context.getTimestamp()))).build());
        // We do not re-save producer id
        properties.add(makeProperty(CONTEXT_OF_COMMAND_PROPERTY_NAME,
                makeValue(Messages.toText(context.getCommandContext()))).build());
        properties.add(makeProperty(CONTEXT_VERSION,
                makeValue(context.getVersion())).build());
        // We do not save attributes
        return properties;
    }

    /**
     * Converts {@link org.spine3.base.Event}'s fields to a set of Properties, which are
     * ready to add to datastore entity.
     */
    /* package */ static Iterable<? extends Property> makeEventFieldProperties(EventStorageRecordOrBuilder event) {
        final Collection<Property> properties = new ArrayList<>();

        // We do not re-save timestamp
        // We do not re-save event type
        properties.add(makeProperty(EVENT_ID_PROPERTY_NAME, makeValue(event.getEventId())).build());
        properties.add(makeProperty(PRODUCER_ID_PROPERTY_NAME, makeValue(event.getProducerId())).build());

        return properties;
    }
}
