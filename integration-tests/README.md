# Integration tests

This directory contains integration test environment used by multiple modules of `gcloud-java`.

## `web-tests`

The `web-tests` module contains a simplistic Spine application.

The application uses the Spine `web` API (with the `firebase-web` library).
 
The application is deployed on the AppEngine `spine-dev` project. The `client-js` tests use 
this application as a test backend.

### Deployment

In order to deploy the app to AppEngine, add the Firebase Admin key to the `resourses` directory and
execute `./gradlew :web-tests:appengineDeploy` task.
