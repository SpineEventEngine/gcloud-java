/*
 * Copyright (c) 2000-2018 TeamDev. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import com.google.protobuf.Timestamp;
import io.spine.base.Time;
import io.spine.client.Query;
import io.spine.client.QueryFactory;
import io.spine.client.TestActorRequestFactory;
import io.spine.core.Ack;
import io.spine.core.Command;
import io.spine.json.Json;
import io.spine.protobuf.AnyPacker;
import io.spine.web.given.QueryServletTestEnv;
import io.spine.web.given.QueryServletTestEnv.TestQueryServlet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.StringWriter;

import static io.spine.core.Status.StatusCase.OK;
import static io.spine.web.given.Servlets.request;
import static io.spine.web.given.Servlets.response;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("QueryServlet should")
class QueryServletTest {

    private static final QueryFactory queryFactory =
            TestActorRequestFactory.newInstance(QueryServletTest.class).query();

    @Test
    @DisplayName("throw UnsupportedOperationException when trying to serialize")
    void testFailToSerialize() throws IOException {
        final QueryServlet servlet = new TestQueryServlet();
        final ObjectOutputStream stream = new ObjectOutputStream(new ByteArrayOutputStream());
        assertThrows(UnsupportedOperationException.class, () -> stream.writeObject(servlet));
        stream.close();
    }

    @Test
    @DisplayName("handle query POST requests")
    void testHandle() throws IOException {
        final Timestamp expectedData = Time.getCurrentTime();
        final QueryServlet servlet = new TestQueryServlet(expectedData);
        final StringWriter response = new StringWriter();
        final Query query = queryFactory.all(Timestamp.class);
        servlet.doPost(request(query), response(response));
        final Timestamp actualData = Json.fromJson(response.toString(), Timestamp.class);
        assertEquals(expectedData, actualData);
    }

    @Test
    @DisplayName("respond 400 to an invalid query")
    void testInvalidCommand() throws IOException {
        final QueryServlet servlet = new TestQueryServlet();
        final HttpServletResponse response = response(new StringWriter());
        servlet.doPost(request(Time.getCurrentTime()), response);
        verify(response).sendError(400);
    }
}
