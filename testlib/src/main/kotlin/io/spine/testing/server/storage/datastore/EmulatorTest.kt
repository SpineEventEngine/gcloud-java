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

package io.spine.testing.server.storage.datastore

import org.junit.jupiter.api.extension.ExtendWith

/**
 * Marks a test class that exercises the Docker-based Datastore Emulator.
 *
 * Such a test is skipped on the **Windows CI** runner, which cannot launch the Linux
 * container that hosts the emulator. That runner signals the limitation by setting the
 * [EmulatorCondition.WINDOWS_CI_NO_DOCKER] environment variable; [EmulatorCondition] reads it
 * and disables the annotated class cleanly — before its `static` initializers (which often
 * open a Datastore connection) would run.
 *
 * In every other environment — developer machines on any OS and the Ubuntu CI runner — Docker
 * is **mandatory**. Its absence there is a build failure raised by the `checkDockerAvailable`
 * Gradle task, not a silent skip, so a green build never hides un-exercised emulator tests.
 *
 * Apply this to every test class that touches the emulator (directly, through a base class, or
 * via a `static` factory field).
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@ExtendWith(EmulatorCondition::class)
public annotation class EmulatorTest
