package io.spine.internal.dependency

// https://github.com/testcontainers/testcontainers-java
//
// Also, see https://java.testcontainers.org/#maven-dependencies.
//
object Testcontainers {
    const val version = "1.19.0"
    private const val group = "org.testcontainers"

    const val lib = "$group:testcontainers:$version"
    const val junitJupiter = "$group:junit-jupiter:$version"

    const val gcloud = "$group:gcloud:$version"
}
