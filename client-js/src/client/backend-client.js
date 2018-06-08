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
 * The client of the application backend.
 *
 * Orchestrates the work of the HTTP and Firebase clients and
 * the {@link ActorRequestFactory}.
 */
export class BackendClient {

    /**
     * Creates a new `BackendClient`.
     *
     * @param httpClient          the {@link HttpClient} to connect to
     *                            the backend with
     * @param firebaseClient      the {@link FirebaseClient} to read the query
     *                            results with
     * @param actorRequestFactory the {@link ActorRequestFactory} to instantiate
     *                            the actor requests with
     */
    constructor(httpClient, firebaseClient, actorRequestFactory) {
        this._httpClient = httpClient;
        this._firebase = firebaseClient;
        this._actorRequestFactory = actorRequestFactory;
    }

    /**
     * Fetches all the entities of the given type.
     *
     * @param type          the target {@link TypeUrl}
     * @param dataCallback  the callback which receives the data, one-by-one, in
     *                      a form of a JS object
     * @param errorCallback the callback which receives the errors
     */
    fetchAll(type, dataCallback, errorCallback = null) {
        let query = this._actorRequestFactory.queryAll(type);
        this._fetch(query, dataCallback, errorCallback);
    }

    /**
     * Fetches a single entity of the given type.
     *
     * @param type          the target {@link TypeUrl}
     * @param id            the target entity ID, as a {@link TypedMessage}
     * @param dataCallback  the callback which receives the single data item, in
     *                      a form of a JS object
     * @param errorCallback the callback which receives the errors
     */
    fetchById(type, id, dataCallback, errorCallback = null) {
        let query = this._actorRequestFactory.queryById(type.value, id);
        this._fetch(query, dataCallback, errorCallback);
    }

    /**
     * Sends the given command to the server.
     *
     * @param commandMessage    the {@link TypedMessage} representing the command
     *                          message
     * @param successListener   the no-argument callback invoked if the command
     *                          is acknowledged
     * @param errorCallback     the callback which receives the errors
     * @param rejectionCallback the callback which receives the command rejections
     */
    sendCommand(commandMessage,
                successListener,
                errorCallback,
                rejectionCallback) {
        let command = this._actorRequestFactory.command(commandMessage);
        this._httpClient.postMessage("/command", command)
            .then(response => response.json())
            .then(ack => {
                let status = ack.status;
                if (status.hasOwnProperty("ok")) {
                    successListener();
                } else if (status.hasOwnProperty("error")) {
                    errorCallback(status.error)
                } else if (status.hasOwnProperty("rejection")) {
                    console.log("Command rejected.");
                    rejectionCallback(status.rejection);
                }
            }, errorCallback);
    }

  _fetch(query, dataCallback, errorCallback=null) {
    let onError = errorCallback || function (e) {};
    this._httpClient.postMessage("/query", query)
        .then(response => response.text())
        .then(path => this._firebase.subscribeTo(path, dataCallback), onError);
  }
}
