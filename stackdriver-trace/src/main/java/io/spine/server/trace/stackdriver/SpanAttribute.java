/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.trace.stackdriver;

import com.google.devtools.cloudtrace.v2.AttributeValue;
import com.google.devtools.cloudtrace.v2.TruncatableString;
import com.google.protobuf.Message;
import io.spine.core.TenantId;
import io.spine.json.Json;
import io.spine.string.Stringifiers;

import static io.spine.protobuf.AnyPacker.unpack;

/**
 * Attributes assigned to a span.
 */
enum SpanAttribute {

    BoundedContext {
        @Override
        AttributeValue value(SignalSpan signalSpan) {
            String fullName = signalSpan.contextName().getValue();
            return SpanAttribute.stringValue(fullName);
        }
    },
    Tenant {
        @Override
        AttributeValue value(SignalSpan signalSpan) {
            TenantId tenantId = signalSpan.signal().tenant();
            String tenantString = Json.toCompactJson(tenantId);
            return SpanAttribute.stringValue(tenantString);
        }
    },
    EntityID {
        @Override
        AttributeValue value(SignalSpan signalSpan) {
            Message id = unpack(signalSpan.receiver().getId());
            String printedId = Stringifiers.toString(id);
            return SpanAttribute.stringValue(printedId);
        }
    },
    SignalID {
        @Override
        AttributeValue value(SignalSpan signalSpan) {
            String printedId = signalSpan.signal().id().value();
            return SpanAttribute.stringValue(printedId);
        }
    },
    EntityType {
        @Override
        AttributeValue value(SignalSpan signalSpan) {
            String entityType = signalSpan.receiver()
                                          .getTypeUrl();
            return SpanAttribute.stringValue(entityType);
        }
    },
    SignalType {
        @Override
        AttributeValue value(SignalSpan signalSpan) {
            String signalType = signalSpan.signal()
                                          .enclosedTypeUrl()
                                          .value();
            return SpanAttribute.stringValue(signalType);
        }
    };

    private static final String ATTRIBUTE_PREFIX = "spine.io";
    private static final int MAX_SIZE = 256;

    /**
     * Obtains the qualified name of the attribute.
     *
     * <p>The name starts with the {@code spine.io/} prefix.
     */
    String qualifiedName() {
        return ATTRIBUTE_PREFIX + '/' + name();
    }

    /**
     * Obtains the value of the attribute.
     */
    abstract AttributeValue value(SignalSpan signalSpan);

    private static AttributeValue stringValue(String value) {
        TruncatableString truncatable = Truncate.stringTo(value, MAX_SIZE);
        AttributeValue attributeValue = AttributeValue
                .newBuilder()
                .setStringValue(truncatable)
                .build();
        return attributeValue;
    }
}
