/*
 * Copyright 2018, TeamDev. All rights reserved.
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

"use strict";

/**
 * The cross-platform HTTP fetch function.
 *
 * This way of performing HTTP requests works both in the browser JavaScript and in the Node.js.
 */
let fetch = require("isomorphic-fetch");

/**
 * The HTTP client which performs the connection to the License Server.
 */
export class HttpClient {

    /**
     * Creates a new instance of HttpClient.
     *
     * @param appBaseUrl the application base URL (the protocol and the domain name) represented as
     *                   a string
     */
    constructor(appBaseUrl) {
        this._appBaseUrl = appBaseUrl;
    }

    /**
     * Sends the given message to the given endpoint.
     *
     * The message is sent as in form of a Base64-encoded byte string.
     *
     * @param endpoint the endpoint to send the message to
     * @param message  the message to send, as a {@link TypedMessage}
     */
    postMessage(endpoint, message) {
        let messageString = message.toBase64();
        let path = endpoint.startsWith("/") ? endpoint : "/" + endpoint;
        let url = this._appBaseUrl + path;
        let request = {
            method: "POST",
            body: messageString,
            headers: {
                'Content-Type': 'application/x-protobuf'
            },
            mode: 'cors'
        };
        let result = fetch(url, request);
        return result;
    }
}
