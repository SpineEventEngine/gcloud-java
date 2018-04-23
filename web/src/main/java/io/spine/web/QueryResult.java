/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * A result of a query processing.
 *
 * <p>The structure of this object is not defined in the general case. It may, for example,
 * be an error message, the data matching the associated query, or neither of those.
 *
 * <p>A query result can be {@linkplain #writeTo(ServletResponse) written} into
 * a {@link ServletResponse} in order to be sent to a client.
 *
 * @author Dmytro Dashenkov
 */
public interface QueryResult {

    /**
     * Writes this {@code QueryResult} into the given {@link ServletResponse}.
     *
     * @param response the response to write the result into
     * @throws IOException in case of a failure
     */
    void writeTo(ServletResponse response) throws IOException;
}
