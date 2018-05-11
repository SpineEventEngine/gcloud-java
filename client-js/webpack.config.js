/*
 * Copyright (c) 2000-2018 TeamDev. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

let path = require("path");

const config = {
  entry: "./src/client/client.js",
  output: {
    path: path.resolve(__dirname, "build"),
    filename: "client-all.js",
    libraryTarget: "this"
  },
  target: "web"
};

module.exports = config;
