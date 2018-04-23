/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;

import static io.spine.util.Exceptions.unsupported;

/**
 * An {@link HttpServlet} which receives {@linkplain QueryParser query requests}, dispatches them
 * into a {@link QueryMediator} and writes the {@linkplain QueryResult mediation result} into
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
 * {@link io.spine.client.Query Query}), the response will contain the {@linkplain QueryResult
 * query mediation result}. Otherwise, the response will be empty with the response code
 * {@link HttpServletResponse#SC_BAD_REQUEST 400}.
 *
 * <p>A typical implementation would extend this class and provide a {@link QueryMediator} in
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
public abstract class QueryServlet extends HttpServlet {

    private final QueryMediator mediator;

    /**
     * Creates a new instance of {@link QueryServlet} with the given {@link QueryMediator}.
     *
     * @param mediator the query mediator to be used in this query servlet
     */
    protected QueryServlet(QueryMediator mediator) {
        super();
        this.mediator = mediator;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Handles the {@code POST} request through the {@link QueryMediator}.
     */
    @OverridingMethodsMustInvokeSuper
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        final Optional<QueryParser> query = QueryParser.from(req);
        if (!query.isPresent()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            final QueryParser queryParser = query.get();
            final QueryResult result = mediator.mediate(queryParser);
            result.writeTo(resp);
        }
    }

    // Disables serialization.
    // ----------------------

    /**
     * Blocks the Java serialization mechanism.
     *
     * <p>It is a common case for a {@code QueryServlet} not to be able to serialize its fields.
     * Thus, any {@code QueryServlet} refuses the serialization bequest.
     *
     * @throws UnsupportedOperationException always
     */
    private void writeObject(ObjectOutputStream ignored) throws UnsupportedOperationException {
        throw unsupported("QueryServlet serialization is not supported.");
    }

    /**
     * Blocks the Java serialization mechanism.
     *
     * @throws UnsupportedOperationException always
     * @see #writeObject(ObjectOutputStream)
     */
    private void readObject(ObjectInputStream ignored) throws UnsupportedOperationException {
        throw unsupported("QueryServlet deserialization is not supported.");
    }
}
