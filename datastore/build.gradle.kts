/*
 * Copyright 2022, TeamDev. All rights reserved.
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
import io.spine.internal.gradle.IncrementGuard
import io.spine.tools.gradle.exec.ExecFork

plugins {
    id("io.spine.execfork")
}

apply<IncrementGuard>()

val spineCoreVersion: String by extra
val spineBaseVersion: String by extra
val spineBaseTypesVersion: String by extra

dependencies {
    // Google Cloud Datastore
    api(GoogleCloud.datastore) {
        exclude(group = "com.google.protobuf")
        exclude(group = "com.google.guava")
    }
    api("io.spine:spine-base:$spineBaseVersion")
    api("io.spine:spine-base-types:$spineBaseTypesVersion")

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

//TODO:2021-07-22:alexander.yevsyukov: Turn to WARN and investigate duplicates.
// see https://github.com/SpineEventEngine/base/issues/657
val dupStrategy = DuplicatesStrategy.INCLUDE
tasks.processResources.get().duplicatesStrategy = dupStrategy
tasks.processTestResources.get().duplicatesStrategy = dupStrategy
tasks.sourceJar.get().duplicatesStrategy = dupStrategy
tasks.jar.get().duplicatesStrategy = dupStrategy
