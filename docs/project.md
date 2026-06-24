# Project: gcloud-jvm

## Overview

`gcloud-jvm` provides Google Cloud integrations for Spine-based server
applications. It adapts the storage extension point of the Spine SDK (notably
[`core-jvm`][core-jvm]) to Google Cloud services — persisting application state
in Cloud Datastore and exposing Cloud Pub/Sub message types. The artifacts are
added to a Spine server application running on the Google Cloud Platform; they
are not used on their own.

Distributed tracing previously lived here as the `stackdriver-trace` module; it
has been superseded by the OpenTelemetry-based `server-otel` module in
[`core-jvm`][core-jvm] (see [`docs/opentelemetry.md`](opentelemetry.md)).

## Architecture

Role: **library** (multi-module Gradle build). The production modules publish
artifacts under the `io.spine.gcloud` group; the `testlib` test-utility module
publishes under the `io.spine.tools` group as `gcloud-testlib`. The modules are:

- `datastore` — a Cloud Datastore-backed implementation of the Spine server
  storage SPI. `DatastoreStorageFactory` and `DatastoreWrapper` provide record,
  aggregate, inbox, catch-up, and event storage, with multitenancy via Datastore
  namespaces and transactional reads and writes.
- `pubsub` — the Cloud Pub/Sub gRPC API and message types.
- `testlib` — test utilities for the modules above, including a
  Testcontainers-based Datastore Emulator (`TestDatastores`,
  `TestDatastoreStorageFactory`) used by the storage tests.

Key patterns and constraints:

- The modules implement Spine server-side extension points (notably
  `StorageFactory`) and are wired into a Spine server app via the
  [Bootstrap plugin][bootstrap] and [`core-jvm`][core-jvm], rather than being
  consumed directly by end users.
- Public API stability matters: consumer applications pin to versions published
  here, so removals and signature changes are breaking. Versioning follows the
  Spine SDK policy (`.agents/guidelines/version-policy.md`).
- The Datastore Emulator tests require a Linux Docker environment and are
  skipped when Docker is unavailable (for example, on the Windows CI runner).
- Dependency declarations live under
  `buildSrc/src/main/kotlin/io/spine/dependency/`.

Read [`.agents/guidelines/jvm-project.md`](../.agents/guidelines/jvm-project.md) for build stack,
coding style, tests, and versioning.

[core-jvm]: https://github.com/SpineEventEngine/core-jvm
[bootstrap]: https://github.com/SpineEventEngine/bootstrap
