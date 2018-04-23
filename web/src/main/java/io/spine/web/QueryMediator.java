/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

/**
 * An {@linkplain io.spine.client.Query entity query} mediator.
 *
 * <p>Translates the given {@link QueryParser} into a {@link QueryResult}.
 *
 * <p>No constrains are applied to the contents of the query. Neither any guaranties are made for
 * the query result. Investigate the concrete implementation to find out the details of their
 * behaviour.
 *
 * @author Dmytro Dashenkov
 */
public interface QueryMediator {

    /**
     * Processes the given {@link QueryParser} and returns a {@link QueryResult}.
     *
     * @param query the query to mediate
     * @return the query result
     */
    QueryResult mediate(QueryParser query);
}
