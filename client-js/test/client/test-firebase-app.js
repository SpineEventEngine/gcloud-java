/*
 * Copyright (c) 2000-2018 TeamDev. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

import firebase from "firebase";

let config = {
    apiKey: "AIzaSyD8Nr2zrW9QFLbNS5Kg-Ank-QIZP_jo5pU",
    authDomain: "spine-dev.firebaseapp.com",
    databaseURL: "https://spine-dev.firebaseio.com",
    projectId: "spine-dev",
    storageBucket: "",
    messagingSenderId: "165066236051"
};
export let devFirebaseApp = firebase.initializeApp(config, "spine-dev");
