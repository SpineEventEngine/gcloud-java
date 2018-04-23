/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web;

import io.spine.client.Query;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Optional;

import static com.teamdev.licensing.web.parser.HttpMessages.parse;

/**
 * A parser for a {@link Query} contained in an {@link HttpServletRequest}.
 *
 * @author Dmytro Dashenkov
 */
public final class QueryParser {

    private final Query query;

    private QueryParser(Query query) {
        this.query = query;
    }

    /**
     * Creates a {@code QueryParser} for the given {@link HttpServletRequest}.
     *
     * <p>The given request should contain a JSON representation of a {@link Query} in its body.
     *
     * @param request the request with a {@link Query}
     * @return a parser if the given request can be parsed into a {@link Query},
     *         {@code Optional.empty()} otherwise
     */
    public static Optional<QueryParser> from(HttpServletRequest request)
            throws IOException {
        return parse(request, Query.class).map(QueryParser::new);
    }

    /**
     * Returns the parsed {@link Query io.spine.client.Query}.
     *
     * @return an instance of {@link Query} to process
     */
    public final Query query() {
        return query;
    }
}
