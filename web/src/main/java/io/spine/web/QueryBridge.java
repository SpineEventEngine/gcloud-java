/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import io.spine.client.Query;

/**
 * An {@linkplain io.spine.client.Query entity query} bridge.
 *
 * <p>Connects the {@link io.spine.server.QueryService QueryService} with a query response
 * processor. Typically, the query response processor is the channel which sends the query response
 * to the client.
 *
 * <p>No constrains are applied to the contents of the query. Neither any guaranties are made for
 * the query result. Refer to the concrete implementations to find out the details of their
 * behaviour.
 *
 * @author Dmytro Dashenkov
 */
public interface QueryBridge {

    /**
     * Sends the given {@link Query} to the {@link io.spine.server.QueryService QueryService} and
     * dispatches the query response to the query response processor.
     *
     * <p>Returns the result of query processing.
     *
     * @param query the query to send
     * @return the query result
     */
    QueryProcessingResult send(Query query);
}
