# gcloud-java

[![codecov][codecov-badge]][codecov]
[![Ubuntu build][ubuntu-build-badge]][gh-actions]


[codecov]: https://codecov.io/gh/SpineEventEngine/gcloud-java
[codecov-badge]: https://codecov.io/gh/SpineEventEngine/gcloud-java/branch/master/graph/badge.svg
[gh-actions]: https://github.com/SpineEventEngine/gcloud-java/actions
[ubuntu-build-badge]: https://github.com/SpineEventEngine/gcloud-java/actions/workflows/build-on-ubuntu-gcloud.yml/badge.svg

 
Support for Spine-based Java apps running at Google Cloud.

### Java Version

Starting version `2.0.0-SNAPSHOT.63`, the artifacts of this library are being built with Java 11
compilation target. Therefore, the consumer applications have to use Java 11 or higher.

Previous versions were build with Java 8.

### Artifacts

Gradle:

```kotlin
// Compatible with Java 8:

dependencies {
    // Datastore Storage support library.
    implementation("io.spine.gcloud:spine-datastore:1.9.0")

    // Pub/Sub messaging support library.
    implementation("io.spine.gcloud:spine-pubsub:1.790")

    // Stackdriver Trace support library.
    implementation("io.spine.gcloud:spine-stackdriver-trace:1.9.0")

    // Datastore-related test utilities (if needed).
    testImplementation("io.spine.gcloud:testutil-gcloud:1.9.0")
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

The library utilizes Testcontainers in order 
to run a [local Datastore emulator](https://java.testcontainers.org/modules/gcloud/#datastore).

Therefore, a local Docker is required up and running, in order to launch tests. 

##### Running the tests

*Datastore and `testutil-gcloud`*

To start a local Docker-based emulator and run test against it, run `./gradlew check`.

Emulator container is re-used across tests. After test run is completed, the emulator container 
shuts down automatically.

Some tests also verify a connection to a remote Datastore instance. In order to run those,
the corresponding credential file called `spine-dev.json` should be placed under
`<project root>/datastore/src/test/resources/` and `<project root>/testutil-gcloud/src/test/resources/`.

Gradle build script is arranged to do that automatically upon running on CI.

*Stackdriver-Trace*

The test are launched in a scope of Gradle `test` phase. However, they rely on a Google Cloud 
credentials file located at `<project root>/stackdriver-trace/src/test/resources/spine-dev.json`.

To run the tests, obtain the service account file for your environment and make it available 
to the test code in the specified locations.
