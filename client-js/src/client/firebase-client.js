/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

"use strict";

/**
 * The client of a Firebase Realtime database.
 */
export class FirebaseClient {

  /**
   * Creates a new FirebaseClient.
   *
   * @param firebaseApp an initialized Firebase app
   */
  constructor(firebaseApp) {
    this._firebaseApp = firebaseApp;
  }

  /**
   * Subscribes to the child_added events of of the node under the given path.
   *
   * Each child's value is parsed as a JSON and dispatched to the given callback
   *
   * @param path         the path to the watched node
   * @param dataCallback the child value callback
   */
  subscribeTo(path, dataCallback) {
    console.log("Subscribe to " + path);
    let dbRef = this._firebaseApp.database().ref(path);
    dbRef.on("child_added", data => {
      let msgJson = data.val();
      let message = JSON.parse(msgJson);
      dataCallback(message);
    });
  }
}
