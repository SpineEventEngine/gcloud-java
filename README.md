# gae-java

[![codecov](https://codecov.io/gh/SpineEventEngine/gae-java/branch/master/graph/badge.svg)](https://codecov.io/gh/SpineEventEngine/gae-java)
[![Build Status](https://travis-ci.org/SpineEventEngine/gae-java.svg?branch=master)](https://travis-ci.org/SpineEventEngine/gae-java)
[![codacy](https://api.codacy.com/project/badge/Grade/fe24ec78520943afa038336d45db4513)](https://www.codacy.com/app/SpineEventEngine/gae-java?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=SpineEventEngine/gae-java&amp;utm_campaign=Badge_Grade)

Support for Spine-based Java apps running under Google App Engine.


##### Testing

###### Preconditions

To run the task successfully, you must have `gcloud` tool properly installed and configured: 
 - install gcloud of the last version
 - login under a Google account when initializing the `gcloud`
 - to run tests you should select `spine-dev` Google Cloud Console project
 - skip Google App Engine setup if not required


###### Unix-like
To start a localhost emulator and run tests run `./gradlew check`.
To start an emulator without running tests `./gradlew startDatastore`.
To stop datastore use standard system means (e.g. `kill -9 $(lsof -i:8080)`).

###### Windows

To start a localhost emulator go to dir `script` and run `start-datastore.bat` as an __administrator__.
The first launch may download and initialize the emulator itself. If so, rerun the script after the install is complete.
To run tests execute `gradlew check`

###### General

The launched emulator will run at `localhost:8080` and will not have any persistence.
To change this configs see `./script/start-datastore.*` scripts.


The datastore is cleaned up after each test.
See test classes under `./gcd/src/test/java/...` and `TestDatastoreStorageFactory#clear`.
