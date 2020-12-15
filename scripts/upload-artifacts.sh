#!/usr/bin/env bash

#
# Copyright 2020, TeamDev. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Redistribution and use in source and/or binary forms, with or without
# modification, must retain the above copyright notice and the following
# disclaimer.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

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
