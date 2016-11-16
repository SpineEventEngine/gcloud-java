# gae-java

[![codecov](https://codecov.io/gh/SpineEventEngine/gae-java/branch/master/graph/badge.svg)](https://codecov.io/gh/SpineEventEngine/gae-java)
[![Build Status](https://travis-ci.org/SpineEventEngine/gae-java.svg?branch=master)](https://travis-ci.org/SpineEventEngine/gae-java)

Support for Spine-based Java apps running under Google App Engine.

To start a localhost emulator and run tests run `./gradlew check`.
To start an emulator without running tests `./gradlew startDatastore`.
To stop datastore use standard system means (e.g. `kill -9 \$(lsof -i:8080)`).
To run the task successfully, you must have `gcloud` tool properly
installed and configured: 
 - install gcloud of the last version
 - login under a Google account when initializing the `gcloud`
 - to run tests you should either select `spine-dev` Google Cloud Console project or create one if it's not accessible for you 
 - if the Google project has different name, change it in `LocalDatastoreStorageFactory`
 
The launched emulator will run in `localhost:8080` and will not have any persistence.
The datastore is cleaned up after each test.
To change this configs see `./script/start-datastore.*` scripts.
