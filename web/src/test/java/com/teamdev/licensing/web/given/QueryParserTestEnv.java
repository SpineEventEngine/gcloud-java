/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web.given;

import com.google.protobuf.Any;
import io.spine.client.Query;
import io.spine.client.TestActorRequestFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public final class QueryParserTestEnv {

    private static final TestActorRequestFactory factory =
            TestActorRequestFactory.newInstance(QueryParserTestEnv.class);

    /**
     * Prevents the utility class instantiation.
     */
    private QueryParserTestEnv() {
    }

    public static HttpServletRequest request(String contents) throws IOException {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getReader()).thenReturn(new BufferedReader(new StringReader(contents)));
        return request;
    }

    public static Query query() {
        return factory.query().all(Any.class);
    }
}
