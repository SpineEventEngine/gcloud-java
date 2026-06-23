#!/usr/bin/env bash

# Copyright 2026, TeamDev. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
gcs_properties="$script_dir/../gcs.properties"
bucket="$(sed -n 's/^artifacts\.bucket=//p' "$gcs_properties")"
folder="$(sed -n 's/^artifacts\.folder=//p' "$gcs_properties")"

BUILD_NAME="$2-$1" # Args: $1 = remote branch name, $2 = build number.

gsutil cp -r "gs://$bucket/$folder/$BUILD_NAME/test-reports.zip" "test-reports/$BUILD_NAME/report.zip"
unzip -aoq "test-reports/$BUILD_NAME/report.zip" -d "test-reports/$BUILD_NAME/"
rm "test-reports/$BUILD_NAME/report.zip"
open "test-reports/$BUILD_NAME"
