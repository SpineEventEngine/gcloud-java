/*
 * Copyright 2018, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore;

import static java.lang.System.getenv;

/**
 * A utility class for analyzing the test environment at runtime.
 */
final class TestEnvironment {

    private static final String TRUE = "true";

    /** Prevents the utility class instantiation. */
    private TestEnvironment() {
    }

    /**
     * Shows if the current test JVM is started within a continuous integration service.
     *
     * <p>This method relies on the convention for the CI services to set the {@code CI}
     * environmental variable to {@code "true"}.
     *
     * @return {@code true} if the tests are run on a CI service, {@code false} otherwise
     * @see System#getenv()
     */
    @SuppressWarnings("CallToSystemGetenv")
    public static boolean runsOnCi() {
        String ciEnvValue = getenv("CI");
        boolean onCi = TRUE.equalsIgnoreCase(ciEnvValue);
        return onCi;
    }
}
