/*
 * Copyright (c) 2000-2018 TeamDev. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import io.spine.client.Query;
import io.spine.web.parser.HttpMessages;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;

/**
 * An {@link HttpServlet} which receives {@linkplain Query query requests}, passes them
 * into a {@link QueryBridge} and writes the {@linkplain QueryProcessingResult sending result} into
 * the response.
 *
 * <p>The servlet supports only {@code POST} requests. {@code GET}, {@code HEAD}, {@code PUT},
 * {@code DELETE}, {@code OPTIONS}, and {@code TRACE} methods are not supported by default.
 *
 * <p>In order to perform a {@linkplain io.spine.client.Query query}, a client should send an HTTP
 * {@code POST} request to this servlet. The request body should contain
 * a {@linkplain io.spine.json.Json JSON} representation of an
 * {@link io.spine.client.Query io.spine.client.Query}.
 *
 * <p>If the request is valid (i.e. the request body contains a valid
 * {@link io.spine.client.Query Query}), the response will contain the {@linkplain QueryProcessingResult
 * query sending result}. Otherwise, the response will be empty with the response code
 * {@link HttpServletResponse#SC_BAD_REQUEST 400}.
 *
 * <p>A typical implementation would extend this class and provide a {@link QueryBridge} in
 * the constructor. No additional config is required in order for this servlet to handle
 * the {@linkplain io.spine.client.Query entity queries}.
 *
 * <p>A {@code QueryServlet} does not support serialization. Please keep that in mind when selecting
 * a servlet container. When trying to serialize an instance of {@code QueryServlet}, an
 * {@link UnsupportedOperationException} is thrown.
 *
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("serial") // Java serialization is not supported.
public abstract class QueryServlet extends NonSerializableServlet {

    private final QueryBridge bridge;

    /**
     * Creates a new instance of {@link QueryServlet} with the given {@link QueryBridge}.
     *
     * @param bridge the query bridge to be used in this query servlet
     */
    protected QueryServlet(QueryBridge bridge) {
        super();
        this.bridge = bridge;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Handles the {@code POST} request through the {@link QueryBridge}.
     */
    @OverridingMethodsMustInvokeSuper
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        final Optional<Query> query = HttpMessages.parse(req, Query.class);
        if (!query.isPresent()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            final Query queryParser = query.get();
            final QueryProcessingResult result = bridge.send(queryParser);
            result.writeTo(resp);
        }
    }
}
