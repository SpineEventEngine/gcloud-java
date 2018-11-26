#!/usr/bin/env bash

# Installs the Google Cloud SDK.
#
# All output is ignored as the installation process is overly verbose.

export CLOUDSDK_CORE_DISABLE_PROMPTS=1
curl https://sdk.cloud.google.com | bash > /dev/null
