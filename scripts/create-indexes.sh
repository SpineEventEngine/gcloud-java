#!/usr/bin/env bash
# Creates Google Cloud Datastore indexes required by datastore module tests.
gcloud datastore indexes create ./datastore/src/test/config/index.yaml
