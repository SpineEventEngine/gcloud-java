/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web;

import com.teamdev.licensing.web.given.QueryParserTestEnv;
import io.spine.client.Query;
import io.spine.json.Json;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("QueryParser should")
class QueryParserTest {

    private Query query;

    @BeforeEach
    void setUp() {
        query = QueryParserTestEnv.query();
    }

    @Test
    @DisplayName("parse query from an HttpServletRequest")
    void testFromJson() throws IOException {
        testParse(Json.toJson(query));
    }

    @Test
    @DisplayName("return nothing if query is empty")
    void testEmpty() throws IOException {
        final HttpServletRequest request = QueryParserTestEnv.request("non parsable request");
        final Optional<?> parsed = QueryParser.from(request);
        assertFalse(parsed.isPresent());
    }

    @Test
    @DisplayName("parse itself from a short-form JSON")
    void testShortForm() throws IOException {
        testParse(Json.toCompactJson(query));
    }

    private void testParse(String json) throws IOException {
        final HttpServletRequest request = QueryParserTestEnv.request(json);
        final Optional<QueryParser> parsed = QueryParser.from(request);
        assertTrue(parsed.isPresent());
        assertEquals(query, parsed.get().query());
    }
}
