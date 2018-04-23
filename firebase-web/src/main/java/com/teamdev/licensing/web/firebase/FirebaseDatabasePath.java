/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web.firebase;

import com.google.common.base.Joiner;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import io.spine.client.Query;
import io.spine.client.QueryId;
import io.spine.core.TenantId;
import io.spine.core.UserId;

import java.util.Collection;
import java.util.LinkedList;
import java.util.regex.Pattern;

/**
 * A path in a Firebase Realtime Database.
 *
 * <p>The path is not aware of the database per se. See {@link #reference(FirebaseDatabase)} to
 * bind this path to a database.
 *
 * @author Dmytro Dashenkov
 */
final class FirebaseDatabasePath {

    private static final Pattern ILLEGAL_DATABASE_PATH_SYMBOL = Pattern.compile("[\\[\\].$#]");
    private static final String SUBSTITUTION_SYMBOL = "-";
    private static final String PATH_DELIMITER = "/";
    private static final String DEFAULT_TENANT = "common";

    private final String path;

    private FirebaseDatabasePath(String path) {
        this.path = path;
    }

    /**
     * Creates an instance of {@code FirebaseDatabasePath} which points to a database node storing
     * the {@link io.spine.client.QueryResponse QueryResponse} to the given {@link Query}.
     *
     * @param query the query to host the response of
     * @return new {@code FirebaseDatabasePath}
     */
    static FirebaseDatabasePath allocateForQuery(Query query) {
        final String path = constructPath(query);
        return new FirebaseDatabasePath(path);
    }

    private static String constructPath(Query query) {
        final String tenantId = tenantIdAsString(query);
        final String actor = actorAsString(query);
        final String queryId = queryIdAsString(query);
        final Collection<String> pathElements = new LinkedList<>();
        if (!tenantId.isEmpty()) {
            pathElements.add(escaped(tenantId));
        }
        if (!actor.isEmpty()) {
            pathElements.add(escaped(actor));
        }
        if (!queryId.isEmpty()) {
            pathElements.add(escaped(queryId));
        }
        final String path = Joiner.on(PATH_DELIMITER)
                                  .join(pathElements);
        return path;
    }

    @SuppressWarnings("UnnecessaryDefault")
    private static String tenantIdAsString(Query query) {
        final TenantId tenantId = query.getContext().getTenantId();
        final TenantId.KindCase kind = tenantId.getKindCase();
        switch (kind) {
            case EMAIL:
                return tenantId.getEmail().getValue();
            case DOMAIN:
                return tenantId.getDomain().getValue();
            case VALUE:
                return tenantId.getValue();
            case KIND_NOT_SET: // Fallthrough intended.
            default:
                return DEFAULT_TENANT;
        }
    }

    private static String actorAsString(Query query) {
        final UserId actor = query.getContext().getActor();
        final String result = actor.getValue();
        return result;
    }

    private static String queryIdAsString(Query query) {
        final QueryId queryId = query.getId();
        final String result = queryId.getValue();
        return result;
    }

    private static String escaped(String dirty) {
        return ILLEGAL_DATABASE_PATH_SYMBOL.matcher(dirty).replaceAll(SUBSTITUTION_SYMBOL);
    }

    /**
     * Retrieves a {@link DatabaseReference} to the location denoted by this path in the given
     * {@linkplain FirebaseDatabase database}.
     */
    DatabaseReference reference(FirebaseDatabase firebaseDatabase) {
        return firebaseDatabase.getReference(path);
    }

    /**
     * Retrieves the string value of this path.
     *
     * @return the database path
     */
    @Override
    public String toString() {
        return path;
    }
}
