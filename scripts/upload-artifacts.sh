#!/bin/bash

# This script uploads the Travis artifacts to Google Cloud Storage.

# Installation of https://github.com/travis-ci/dpl.
gem install dpl
# Prepare the test and coverage reports for the upload.
mkdir reports

# Find all directories matching path to add to archive. 
BUILD_REPORTS=$(find . -type d -path "*build/reports*")
JACOCO_REPORTS=$(find . -type d -path "*build/jacoco*")

zip -r reports/test-reports.zip $BUILD_REPORTS
zip -r reports/jacoco-reports.zip $JACOCO_REPORTS

# Returns the value for the specified key.
function getProp() {
    grep "${1}" config/gcs.properties | cut -d'=' -f2
}

# Figure out the target folder name from the name of remote origin repository.
#   (inspired by https://stackoverflow.com/q/8190392/2395775)
folderName=$( git remote -v | head -n1 | awk '{print $2}' | sed 's/.*\///' | sed 's/\.git//' )

# Upload the prepared reports to GCS.
dpl --provider=gcs \
    --access-key-id=GOOG5LZULEBFFFGGPA2G \
    --secret-access-key=${GCS_SECRET} \
    --bucket="$(getProp 'artifacts.bucket')" \
    --upload-dir="$folderName/builds"/${TRAVIS_BUILD_NUMBER}-${TRAVIS_PULL_REQUEST_BRANCH:-$TRAVIS_BRANCH} \
    --local-dir=reports \
    --skip_cleanup=true
