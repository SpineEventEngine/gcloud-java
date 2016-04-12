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

import com.google.api.services.datastore.DatastoreV1;
import com.google.protobuf.Message;
import com.google.protobuf.TimestampOrBuilder;
import org.spine3.protobuf.Messages;

import java.util.Date;

import static com.google.api.services.datastore.client.DatastoreHelper.makeProperty;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;
import static org.spine3.protobuf.Timestamps.convertToDate;
import static org.spine3.protobuf.Timestamps.convertToNanos;

/**
 * @author Mikhail Mikhaylov
 */
@SuppressWarnings("UtilityClass")
/* package */ class DatastoreProperties {

    private static final String TIMESTAMP_PROPERTY_NAME = "timestamp";
    /* package */ static final String TIMESTAMP_NANOS_PROPERTY_NAME = "timestamp_nanos";

    /* package */ static final String AGGREGATE_ID_PROPERTY_NAME = "aggregate_id";
    /* package */ static final String EVENT_TYPE_PROPERTY_NAME = "event_type";

    private DatastoreProperties() {
    }

    /**
     * Makes a property from the given timestamp using {@link org.spine3.protobuf.Timestamps#convertToDate(TimestampOrBuilder)}.
     */
    /* package */ static DatastoreV1.Property.Builder makeTimestampProperty(TimestampOrBuilder timestamp) {
        final Date date = convertToDate(timestamp);
        return makeProperty(TIMESTAMP_PROPERTY_NAME, makeValue(date));
    }

    /* package */ static DatastoreV1.Property.Builder makeTimestampNanosProperty(TimestampOrBuilder timestamp) {
        final long nanos = convertToNanos(timestamp);
        return makeProperty(TIMESTAMP_NANOS_PROPERTY_NAME, makeValue(nanos));
    }

    /* package */ static DatastoreV1.Property.Builder makeAggregateIdProperty(Message aggregateId) {
        final String propertyValue = Messages.toText(aggregateId);
        return makeProperty(AGGREGATE_ID_PROPERTY_NAME, makeValue(propertyValue));
    }

    /* package */ static DatastoreV1.Property.Builder makeEventTypeProperty(String eventType) {
        return makeProperty(EVENT_TYPE_PROPERTY_NAME, makeValue(eventType));
    }
}
