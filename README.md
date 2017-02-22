# gae-java

[![codecov](https://codecov.io/gh/SpineEventEngine/gae-java/branch/master/graph/badge.svg)](https://codecov.io/gh/SpineEventEngine/gae-java)
[![Build Status](https://travis-ci.org/SpineEventEngine/gae-java.svg?branch=master)](https://travis-ci.org/SpineEventEngine/gae-java)
[![codacy](https://api.codacy.com/project/badge/Grade/fe24ec78520943afa038336d45db4513)](https://www.codacy.com/app/SpineEventEngine/gae-java?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=SpineEventEngine/gae-java&amp;utm_campaign=Badge_Grade)

Support for Spine-based Java apps running under Google App Engine.

#### Set up
 
To start using **Spine GAE-Java** implementation please configure your `WEB-INF/datastore-indexes.xml` file:

```xml
<datastore-indexes
        autoGenerate="true">
        
    <!-- your custom indexes -->
        
    <datastore-index kind="org.spine3.server.event.storage.EventStorageRecord"
            ancestor="false">
        <property name="event_type" direction="asc" />
        <property name="timestamp_nanos" direction="asc" />
    </datastore-index>
</datastore-indexes>
```

This helps us to perform more efficient queries to the Cloud Datastore.

#### Testing

##### Preconditions

To run the task successfully, you must have `gcloud` tool properly installed and configured: 
 - install gcloud of the last version;
 - login under a Google account when initializing the `gcloud`;
 - to run tests you should select `spine-dev` Google Cloud Console project;
 - skip Google App Engine setup if not required.


##### Executing the tests

To start a local emulator and run test against it, run `./gradlew check`.

To start an emulator without running tests `./gradlew startDatastore`.

To stop the Datastore emulator, just terminate the emulator process (e.g. `kill -9 $(lsof -i:8080)` or just close the terminal window on Windows).

The launched emulator will run at `localhost:8080` and will not have any persistence.
To change the configuration see `./script/start-datastore.*` scripts.

The datastore is cleaned up after each test.
See test classes under `./gcd/src/test/java/...` and `TestDatastoreStorageFactory#clear`.
