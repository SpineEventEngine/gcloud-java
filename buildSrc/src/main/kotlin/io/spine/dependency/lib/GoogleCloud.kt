/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.spine.dependency.lib

/**
 * https://github.com/googleapis/google-cloud-java
 */
@Suppress("unused", "ConstPropertyName")
object GoogleCloud {

    // https://github.com/googleapis/google-cloud-java/tree/main/sdk-platform-java/java-core
    const val core = "com.google.cloud:google-cloud-core:2.71.0"

    // https://github.com/googleapis/google-cloud-java/tree/main/java-pubsub/proto-google-cloud-pubsub-v1
    const val pubSubGrpcApi = "com.google.api.grpc:proto-google-cloud-pubsub-v1:1.151.0"

    // https://github.com/googleapis/google-cloud-java/tree/main/java-trace
    const val trace = "com.google.cloud:google-cloud-trace:2.93.0"

    // https://github.com/googleapis/google-cloud-java/tree/main/java-datastore
    const val datastore = "com.google.cloud:google-cloud-datastore:2.31.2"
}
