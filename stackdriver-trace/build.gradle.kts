/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import io.spine.internal.dependency.GoogleCloud

dependencies {
    api(GoogleCloud.trace)

    testImplementation("com.google.auth:google-auth-library-oauth2-http:1.3.0")
    testImplementation("io.grpc:grpc-auth") {
        version {
            strictly("1.43.1")
        }
    }

    testImplementation("com.google.auth:google-auth-library-credentials") {
        version {
            strictly("1.3.0")
        }
    }
    testImplementation("org.apache.httpcomponents:httpcore") {
        version {
            strictly("4.4.14")
        }
    }
    testImplementation("commons-codec:commons-codec") {
        version {
            strictly("1.15")
        }
    }
}

//TODO:2021-07-22:alexander.yevsyukov: Turn to WARN and investigate duplicates.
// see https://github.com/SpineEventEngine/base/issues/657
val dupStrategy = DuplicatesStrategy.INCLUDE
tasks.processResources.get().duplicatesStrategy = dupStrategy
tasks.processTestResources.get().duplicatesStrategy = dupStrategy
tasks.sourceJar.get().duplicatesStrategy = dupStrategy
tasks.jar.get().duplicatesStrategy = dupStrategy
