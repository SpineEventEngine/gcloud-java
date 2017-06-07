# gae-java

[![codecov](https://codecov.io/gh/SpineEventEngine/gae-java/branch/master/graph/badge.svg)](https://codecov.io/gh/SpineEventEngine/gae-java)
[![Build Status](https://travis-ci.org/SpineEventEngine/gae-java.svg?branch=master)](https://travis-ci.org/SpineEventEngine/gae-java)
[![codacy](https://api.codacy.com/project/badge/Grade/fe24ec78520943afa038336d45db4513)](https://www.codacy.com/app/SpineEventEngine/gae-java?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=SpineEventEngine/gae-java&amp;utm_campaign=Badge_Grade)

Support for Spine-based Java apps running under Google App Engine.

### Usage

This section describes the main aspects of using the library.

#### Datastore indexes

To work properly, datastore requires to configure the indexes. For the guide, visit [Google Cloud Platform Docs](https://cloud.google.com/datastore/docs/tools/indexconfig).

##### Spine internal indexes

To use `gae-java`, you should configure the Datastore indexes for the Spine internal record types. 
Following index config may be found in `./gcd/src/test/index.yaml`:

```yaml
indexes:

  # Your custom indexes if necessary.

  - kind: spine.base.Event
    ancestor: no
    properties:
    - name: type
    - name: created
```

##### Custom indexes

If you use the Entity Columns feature, you may want to create some custom datastore indexes.

__Example:__
Assuming you have a Projection type called `CustomerProjection`. It's state is declared in 
the Protobuf type `my.company.Customer`. It has Entity Columns `country` and
`companySize`. Once you try to make a query in those Columns, the Datastore will fail with 
an internal Exception. To prevent this, you should create an index for your `CustomerProjection`:
```yaml
- kind: my.company.Customer
    ancestor: no
    properties:
    - name: country
    - name: companySize
```

### Testing

This section describes testing the `gae-java` library itself.

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
See test classes under `./gcd/src/test/java/...` and `io.spine.server.storage.datastore.TestDatastoreStorageFactory#clear`.
