/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

"use strict";

/**
 * The cross-platform HTTP fetch function.
 *
 * This way of performing HTTP requests works both in the browser JavaScript
 * and in the Node.js.
 */
let fetch = require("isomorphic-fetch");

/**
 * The HTTP client which performs the connection to the License Server.
 */
export class HttpClient {

  /**
   * Creates a new instance of HttpClient.
   *
   * @param appBaseUrl the application base URL (the protocol and
   *                   the domain name) represented as a string
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
    let result = fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-protobuf"
      },
      body: messageString
    });
    return result;
  }
}
