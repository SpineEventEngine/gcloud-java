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

import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled
import org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled
import org.junit.jupiter.api.extension.ExecutionCondition
import org.junit.jupiter.api.extension.ExtensionContext

/**
 * Disables [EmulatorTest]-annotated classes on the Windows CI runner.
 *
 * GitHub-hosted Windows runners cannot launch the Linux Docker container that hosts the
 * Datastore Emulator, so the Windows CI job sets the [WINDOWS_CI_NO_DOCKER] environment
 * variable. When it is present, the emulator tests are skipped here; the remaining
 * (non-emulator) unit tests still run, keeping the Windows job a meaningful compilation and
 * unit-test check.
 *
 * This condition does **not** probe Docker. On the Windows runner `docker info` is both
 * unreliable and irrelevant (a running Windows daemon still cannot host the Linux emulator).
 * Everywhere else, Docker is required and its presence is enforced by the `checkDockerAvailable`
 * Gradle task — which reads the same environment variable.
 */
public class EmulatorCondition : ExecutionCondition {

    override fun evaluateExecutionCondition(context: ExtensionContext): ConditionEvaluationResult =
        if (emulatorAvailable()) {
            enabled("The Datastore Emulator is expected to be available.")
        } else {
            disabled(
                "Disabled on the Windows CI runner (`$WINDOWS_CI_NO_DOCKER`), which cannot " +
                    "launch the Linux container hosting the Datastore Emulator."
            )
        }

    private fun emulatorAvailable(): Boolean =
        !System.getenv(WINDOWS_CI_NO_DOCKER).toBoolean()

    public companion object {

        /**
         * The name of the environment variable the Windows CI job sets to mark the runner as
         * unable to launch the Docker-based Datastore Emulator.
         *
         * Part of the testing API so the CI workflow and other tooling can refer to the
         * canonical name. Kept in sync with the `checkDockerAvailable` Gradle task in the root
         * `build.gradle.kts`, which honors the same variable.
         */
        public const val WINDOWS_CI_NO_DOCKER: String = "WINDOWS_CI_NO_DOCKER"
    }
}
