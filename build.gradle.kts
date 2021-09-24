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

import io.spine.internal.dependency.ErrorProne
import io.spine.internal.dependency.JUnit
import io.spine.internal.gradle.PublishingRepos
import io.spine.internal.gradle.Scripts
import io.spine.internal.gradle.applyStandard
import io.spine.internal.gradle.excludeProtobufLite
import io.spine.internal.gradle.forceVersions
import io.spine.internal.gradle.spinePublishing


@Suppress("RemoveRedundantQualifierName") // Cannot use imported things here.
buildscript {
    apply(from = "$rootDir/version.gradle.kts")
    io.spine.internal.gradle.doApplyStandard(repositories)
    io.spine.internal.gradle.doForceVersions(configurations)

    val spineBaseVersion: String by extra

    @Suppress("LocalVariableName")  // For better readability.
    val Kotlin = io.spine.internal.dependency.Kotlin

    dependencies {
        classpath("io.spine.tools:spine-mc-java:$spineBaseVersion")
    }

    configurations.all {
        resolutionStrategy {
            force(
                Kotlin.stdLib,
                Kotlin.stdLibCommon
            )
        }
    }
}

plugins {
    `java-library`
    kotlin("jvm") version io.spine.internal.dependency.Kotlin.version
    idea
    io.spine.internal.dependency.Protobuf.GradlePlugin.apply {
        id(id) version version
    }
    @Suppress("RemoveRedundantQualifierName") // Cannot use imported things here.
    io.spine.internal.dependency.ErrorProne.GradlePlugin.apply {
        id(id) version version
    }
}

spinePublishing {
    targetRepositories.addAll(setOf(
        PublishingRepos.cloudRepo,
        PublishingRepos.gitHub("gcloud-java")
    ))
    projectsToPublish.addAll(
        "datastore",
        "stackdriver-trace",
        "testutil-gcloud",
        "pubsub"
    )
}

val spineBaseVersion: String by extra

allprojects {
    apply(from = "$rootDir/version.gradle.kts")

    apply {
        plugin("jacoco")
        plugin("idea")
    }

    group = "io.spine.gcloud"
    version = extra["versionToPublish"]!!

    repositories.applyStandard()
}

subprojects {

    apply {
        plugin("java-library")
        plugin("com.google.protobuf")
        plugin("net.ltgt.errorprone")
        plugin("io.spine.mc-java")
        plugin("kotlin")
        plugin("pmd")
        plugin("maven-publish")

        with(Scripts) {
            from(javacArgs(project))
            from(projectLicenseReport(project))
            from(slowTests(project))
            from(testOutput(project))
            from(javadocOptions(project))
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    kotlin {
        explicitApi()
    }

    // Required to fetch `androidx.annotation:annotation:1.1.0`,
    // which is a transitive dependency of `com.google.cloud:google-cloud-datastore`.
    repositories {
        google()
    }

    val gRpc = io.spine.internal.dependency.Grpc

    configurations.forceVersions()
    configurations {
        all {
            resolutionStrategy {
                force(
                    "io.grpc:grpc-api:${gRpc.version}",
                    "io.grpc:grpc-protobuf-lite:${gRpc.version}",
                    gRpc.core,
                    gRpc.context,
                    gRpc.stub,
                    gRpc.protobuf,

                    "io.perfmark:perfmark-api:0.23.0",

                    "com.google.api.grpc:proto-google-common-protos:2.2.1",

                    "io.spine:spine-base:$spineBaseVersion",
                    "io.spine.tools:spine-testlib:$spineBaseVersion"
                )
            }
        }
    }
    configurations.excludeProtobufLite()

    val spineCoreVersion: String by extra

    dependencies {
        ErrorProne.apply {
            errorprone(core)
            errorproneJavac(javacPlugin)
        }

        implementation("io.spine:spine-server:$spineCoreVersion")

        testImplementation(JUnit.runner)
        testImplementation("io.spine.tools:spine-testutil-server:$spineCoreVersion")
        testImplementation(group = "io.spine",
                           name = "spine-server",
                           version = spineCoreVersion,
                           classifier = "test")
    }

    val sourcesRootDir = "$projectDir/src"
    val generatedRootDir = "$projectDir/generated"
    val generatedJavaDir = "$generatedRootDir/main/java"
    val generatedTestJavaDir = "$generatedRootDir/test/java"
    val generatedGrpcDir = "$generatedRootDir/main/grpc"
    val generatedTestGrpcDir = "$generatedRootDir/test/grpc"
    val generatedSpineDir = "$generatedRootDir/main/spine"
    val generatedTestSpineDir = "$generatedRootDir/test/spine"

    sourceSets {
        main {
            java.srcDirs(generatedJavaDir, "$sourcesRootDir/main/java", generatedSpineDir)
            resources.srcDir("$generatedRootDir/main/resources")
            proto.srcDirs("$sourcesRootDir/main/proto")
        }
        test {
            java.srcDirs(generatedTestJavaDir, "$sourcesRootDir/test/java", generatedTestSpineDir)
            resources.srcDir("$generatedRootDir/test/resources")
            proto.srcDir("$sourcesRootDir/test/proto")
        }
    }

    val copyCredentials by tasks.registering(Copy::class) {
        val resourceDir = "$projectDir/src/test/resources"
        val fileName = "spine-dev.json"
        val sourceFile = file("$rootDir/$fileName")

        from(sourceFile)
        into(resourceDir)
    }

    tasks.processTestResources {
        dependsOn(copyCredentials)
    }

    tasks.test {
        useJUnitPlatform {
            includeEngines("junit-jupiter")
        }
    }

    tasks.register("sourceJar", Jar::class) {
        from(sourceSets.main.get().allJava)
        archiveClassifier.set("sources")
    }

    tasks.register("testOutputJar", Jar::class) {
        from(sourceSets.test.get().output)
        archiveClassifier.set("test")
    }

    tasks.register("javadocJar", Jar::class) {
        from("$projectDir/build/docs/javadoc")
        archiveClassifier.set("javadoc")
        dependsOn(tasks.javadoc)
    }

    // Apply the same IDEA module configuration for each of sub-projects.
    idea {
        module {
            generatedSourceDirs.addAll(files(
                generatedJavaDir,
                generatedGrpcDir,
                generatedSpineDir,
                generatedTestJavaDir,
                generatedTestGrpcDir,
                generatedTestSpineDir
            ))
            testSourceDirs.add(file(generatedTestJavaDir))

            isDownloadJavadoc = true
            isDownloadSources = true
        }
    }

    apply(from = Scripts.updateGitHubPages(project))
    afterEvaluate {
        tasks.getByName("publish").dependsOn("updateGitHubPages")
    }

    apply(from = Scripts.pmd(project))
}

apply {
    with(Scripts) {
        // Aggregated coverage report across all subprojects.
        from(jacoco(project))
        // Generate a repository-wide report of 3rd-party dependencies and their licenses.
        from(repoLicenseReport(project))
        // Generate a `pom.xml` file containing first-level dependency of all projects
        // in the repository.
        from(generatePom(project))
    }
}
