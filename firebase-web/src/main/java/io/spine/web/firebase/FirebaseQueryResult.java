/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.firebase;

import io.spine.web.QueryResult;

import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * A result of a query processed by a {@link FirebaseQueryBridge}.
 *
 * <p>This result represents a database path to the requested data.
 * See {@link FirebaseQueryBridge} for more details.
 *
 * @author Dmytro Dashenkov
 */
final class FirebaseQueryResult implements QueryResult {

    private static final String MIME_TYPE = "text/plain";

    private final FirebaseDatabasePath path;

    FirebaseQueryResult(FirebaseDatabasePath path) {
        this.path = path;
    }

    @Override
    public void writeTo(ServletResponse response) throws IOException {
        final String databaseUrl = path.toString();
        response.getWriter().append(databaseUrl);
        response.setContentType(MIME_TYPE);
    }

    @Override
    public String toString() {
        return path.toString();
    }
}
