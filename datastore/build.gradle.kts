/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.github.psxpaul.task.ExecFork
import io.spine.gradle.internal.IncrementGuard

plugins {
    id("com.github.psxpaul.execfork").version("0.1.13")
}

apply<IncrementGuard>()

val spineCoreVersion: String by extra
val cloudDatastoreVersion: String by extra

dependencies {
    // Google Cloud Datastore
    api("com.google.cloud:google-cloud-datastore:$cloudDatastoreVersion") {
        exclude(group = "com.google.protobuf")
        exclude(group = "com.google.guava")
    }

    testImplementation(project(":testutil-gcloud"))
    testImplementation("io.spine:spine-server:$spineCoreVersion")
}

val startDatastore by tasks.registering(ExecFork::class) {
    description = "Starts local in-memory datastore."
    group = "Build Setup"
    shouldRunAfter(tasks.assemble)

    val runsOnWindows = org.gradle.internal.os.OperatingSystem.current().isWindows()
    val extension = if (runsOnWindows) "bat" else "sh"
    executable = "$rootDir/scripts/start-datastore.$extension"

    // Default port for the emulator is 8081.
    //
    // See: https://cloud.google.com/sdk/gcloud/reference/beta/emulators/datastore/start
    //
    waitForPort = 8081

    standardOutput = "$buildDir/ds-emulator/stdout.log"
    errorOutput = "$buildDir/ds-emulator/stderr.log"

    // Suppress a console warning for JRE < 9
    killDescendants = false
}

tasks.withType(Test::class) { dependsOn(startDatastore) }
