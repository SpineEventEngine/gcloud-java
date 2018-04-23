/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("QueryServlet should")
class QueryServletTest {

    @Test
    @DisplayName("throw UnsupportedOperationException when trying to serialize")
    void testFailToSerialize() throws IOException {
        final QueryServlet servlet = new TestServlet();
        final ObjectOutputStream stream = new ObjectOutputStream(new ByteArrayOutputStream());
        assertThrows(UnsupportedOperationException.class, () -> stream.writeObject(servlet));
        stream.close();
    }

    @SuppressWarnings("serial")
    private static final class TestServlet extends QueryServlet {

        private TestServlet() {
            super(Mockito.mock(QueryMediator.class));
        }
    }
}
