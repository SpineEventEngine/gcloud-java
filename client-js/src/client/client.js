/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

/**
 * The object which represents the public API of the `client-js` module.
 *
 * This object is exported from the artifact built by webpack.
 *
 * @type {{BackendClient: BackendClient, HttpClient: HttpClient, FirebaseClient: FirebaseClient, ActorRequestFactory: ActorRequestFactory}}
 */
export const client = {
  BackendClient: require("./backend-client").BackendClient,
  HttpClient: require("./http-client").HttpClient,
  FirebaseClient: require("./firebase-client").FirebaseClient,
  ActorRequestFactory: require("./actor-request-factory").ActorRequestFactory
};
