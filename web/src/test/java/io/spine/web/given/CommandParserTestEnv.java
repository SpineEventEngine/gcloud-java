/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web.given;

import com.google.protobuf.Empty;
import io.spine.client.TestActorRequestFactory;
import io.spine.core.Command;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public final class CommandParserTestEnv {

    private static final TestActorRequestFactory factory =
            TestActorRequestFactory.newInstance(CommandParserTestEnv.class);

    /**
     * Prevents the utility class instantiation.
     */
    private CommandParserTestEnv() {
    }

    public static HttpServletRequest request(String contents) throws IOException {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getReader()).thenReturn(new BufferedReader(new StringReader(contents)));
        return request;
    }

    public static Command command() {
        return factory.command().create(Empty.getDefaultInstance());
    }
}
