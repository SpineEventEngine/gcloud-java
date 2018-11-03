#!/usr/bin/env bash

# Downloads the Travis build results from Google Cloud Storage and opens the directory with them.
# The report is downloaded to `test-reports` directory.
#
# First argument is the remote branch name and the second argument is the build number to download.

BUILD_NAME="$2-$1"

gsutil cp -r "gs://spine-dev.appspot.com/gcloud-java/builds/$BUILD_NAME/test-reports.zip" "test-reports/$BUILD_NAME/report.zip" 
unzip -aoq "test-reports/$BUILD_NAME/report.zip" -d "test-reports/$BUILD_NAME/"
rm "test-reports/$BUILD_NAME/report.zip"
open "test-reports/$BUILD_NAME"
