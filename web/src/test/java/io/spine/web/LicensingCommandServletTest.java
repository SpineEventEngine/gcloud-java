/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import io.spine.server.BoundedContext;
import io.spine.server.CommandService;
import io.spine.testdata.TestBoundedContextFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("LicensingCommandServlet should")
class LicensingCommandServletTest {

    private CommandService commandService;

    @BeforeEach
    void setUp() {
        final BoundedContext bc = TestBoundedContextFactory.MultiTenant.newBoundedContext();
        commandService = CommandService.newBuilder()
                                       .add(bc)
                                       .build();
    }

    @Test
    @DisplayName("fail to serialize")
    void testSerialize() throws IOException {
        final CommandServlet servlet = new CommandServlet(commandService) {};
        final ObjectOutputStream stream = new ObjectOutputStream(new ByteArrayOutputStream());
        assertThrows(UnsupportedOperationException.class, () -> stream.writeObject(servlet));
    }
}
