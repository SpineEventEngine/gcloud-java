# gcloud-java

[![codecov][codecov-badge]][codecov] [![Build Status][travis-badge]][travis]

[codecov]: https://codecov.io/gh/SpineEventEngine/gcloud-java
[codecov-badge]: https://codecov.io/gh/SpineEventEngine/gcloud-java/branch/master/graph/badge.svg
[travis]: https://travis-ci.com/SpineEventEngine/gcloud-java
[travis-badge]: https://travis-ci.com/SpineEventEngine/gcloud-java.svg?branch=master
 
Support for Spine-based Java apps running at Google Cloud.

### Artifacts

Gradle:

```kotlin
dependencies {
    // Datastore Storage support library.
    implementation("io.spine.gcloud:spine-datastore:1.9.1")

    // Pub/Sub messaging support library.
    implementation("io.spine.gcloud:spine-pubsub:1.9.1")

    // Stackdriver Trace support library.
    implementation("io.spine.gcloud:spine-stackdriver-trace:1.9.1")

    // Datastore-related test utilities (if needed).
    implementation("io.spine.gcloud:testutil-gcloud:1.9.1")
}
```

These artifacts should be used as a part of the Spine server application.
 
For the details on setting up the server environment please refer to
[Spine Bootstrap Gradle plugin][bootstrap] and [Spine `core` modules][core-java] documentation. 

[bootstrap]: https://github.com/SpineEventEngine/bootstrap/
[core-java]: https://github.com/SpineEventEngine/core-java/

### Configuring Datastore

#### Datastore indexes

In order to run the application built on top of `gcloud-java`, Datastore instance requires some 
preliminary configuration. In particular, the indexes for the Spine internal record types should 
be set. Please notice a special index configuration for your custom `Aggregate` types.

The configuration file is located at `./datastore/config/index.yaml`. 

Please see the [Google Cloud Platform documentation][datastore-index] for the details.

[datastore-index]: https://cloud.google.com/datastore/docs/tools/indexconfig

##### Custom indexes

It is possible to store some of the Spine `Entity` fields in separate Datastore kind fields. 
Such an approach is useful to optimize read-side querying. In this case more Datastore indexes may
 be created.

__Example:__
Assuming you have a Projection type called `CustomerProjection`. Its state is declared in 
the Protobuf type `my.company.Customer`. It has Entity Columns `country` and `companySize`. 
Once you try to make a query in those Columns, the Datastore will fail with 
an internal Exception. To prevent this, you should create an index for your `CustomerProjection`:

```yaml
- kind: my.company.Customer
    ancestor: no
    properties:
    - name: country
    - name: companySize
```

### Testing

This section describes testing the `gcloud-java` library itself.

##### Preconditions

To run the task successfully, you must have `gcloud` tool properly installed and configured: 
 - [install][gcloud-install] `gcloud` of the last version;
 - login under a Google account when initializing the `gcloud`;
 - to run tests you should select `spine-dev` Google Cloud Console project;
 - skip Google App Engine setup if not required.

[gcloud-install]: https://cloud.google.com/sdk/docs/downloads-interactive

##### Running the tests

*Datastore*

To start a local emulator and run test against it, run `./gradlew check`.

To start an emulator without running tests `./gradlew startDatastore`.

To stop the Datastore emulator, just terminate the emulator process (e.g. `kill -9 $(lsof -i:8080)` 
or just close the terminal window on Windows).

The launched emulator will run at `localhost:8080` and will not have any persistence.
To change the configuration see `./scripts/start-datastore.*` scripts.

The datastore is cleaned up after each test.
See test classes under `./datastore/src/test/java/...` and 
`io.spine.server.storage.datastore.TestDatastoreStorageFactory#clear`.

*Stackdriver-Trace*

The test are launched in a scope of Gradle `test` phase. However, they rely on a Google Cloud 
credentials file located at `<project root>/stackdriver-trace/src/test/resources/spine-dev.json`.

To run the tests, obtain the service account file for your environment and make it available 
to the test code in the specified location.
