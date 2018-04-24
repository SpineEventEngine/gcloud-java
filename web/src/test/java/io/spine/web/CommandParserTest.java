/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package io.spine.web;

import io.spine.core.Command;
import io.spine.json.Json;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Optional;

import static io.spine.web.given.CommandParserTestEnv.command;
import static io.spine.web.given.CommandParserTestEnv.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("CommandParser should")
class CommandParserTest {
    
    private Command command;

    @BeforeEach
    void setUp() {
        command = command();
    }

    @Test
    @DisplayName("parse command from an HttpServletRequest")
    void testFromJson() throws IOException {
        testParse(Json.toJson(command));
    }

    @Test
    @DisplayName("return nothing if command is empty")
    void testEmpty() throws IOException {
        final HttpServletRequest request = request("bla bla");
        final Optional<?> parsed = CommandParser.from(request);
        assertFalse(parsed.isPresent());
    }

    @Test
    @DisplayName("parse command from a short-form JSON")
    void testShortForm() throws IOException {
        testParse(Json.toCompactJson(command));
    }

    private void testParse(String json) throws IOException {
        final HttpServletRequest request = request(json);
        final Optional<CommandParser> parsed = CommandParser.from(request);
        assertTrue(parsed.isPresent());
        assertEquals(command, parsed.get().command());
    }
}
