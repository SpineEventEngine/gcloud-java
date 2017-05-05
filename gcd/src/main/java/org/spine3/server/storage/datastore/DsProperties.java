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
import com.google.protobuf.Message;
import org.spine3.string.Stringifiers;
import org.spine3.server.storage.LifecycleFlagField;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.aggregate.storage.AggregateField.aggregate_id;
import static org.spine3.server.storage.LifecycleFlagField.archived;

/**
 * Utility class, which simplifies creation of the Datastore properties.
 *
 * @author Mikhail Mikhaylov
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
class DsProperties {

    private DsProperties() {
    }

    /**
     * Makes AggregateId property from given {@link Message} value.
     */
    static void addAggregateIdProperty(Object aggregateId, Entity.Builder entity) {
        final String propertyValue = Stringifiers.toString(aggregateId);
        entity.set(aggregate_id.toString(), propertyValue);
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
}
