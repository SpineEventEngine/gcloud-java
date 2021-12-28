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

import io.spine.internal.dependency.Apache
import io.spine.internal.dependency.ErrorProne
import io.spine.internal.dependency.GoogleApis
import io.spine.internal.dependency.Grpc
import io.spine.internal.dependency.JUnit
import io.spine.internal.dependency.PerfMark
import io.spine.internal.gradle.JavadocConfig
import io.spine.internal.gradle.applyStandard
import io.spine.internal.gradle.checkstyle.CheckStyleConfig
import io.spine.internal.gradle.excludeProtobufLite
import io.spine.internal.gradle.forceVersions
import io.spine.internal.gradle.github.pages.updateGitHubPages
import io.spine.internal.gradle.javac.configureErrorProne
import io.spine.internal.gradle.javac.configureJavac
import io.spine.internal.gradle.publish.PublishingRepos
import io.spine.internal.gradle.publish.spinePublishing
import io.spine.internal.gradle.report.coverage.JacocoConfig
import io.spine.internal.gradle.report.license.LicenseReporter
import io.spine.internal.gradle.report.pom.PomGenerator
import io.spine.internal.gradle.testing.configureLogging
import io.spine.internal.gradle.testing.registerTestTasks
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

@Suppress("RemoveRedundantQualifierName") // Cannot use imported things here.
buildscript {
    apply(from = "$rootDir/version.gradle.kts")
    io.spine.internal.gradle.doApplyStandard(repositories)

    val execForkPlugin = io.spine.internal.dependency.ExecForkPlugin
    repositories {
        val repos = io.spine.internal.gradle.publish.PublishingRepos
        repos.gitHub(execForkPlugin.repository)
    }

    val mcJavaVersion: String by extra

    dependencies {
        classpath("io.spine.tools:spine-mc-java:$mcJavaVersion")
        classpath(execForkPlugin.classpath)
    }

    io.spine.internal.gradle.doForceVersions(configurations)

    @Suppress("LocalVariableName")  // For better readability.
    val Kotlin = io.spine.internal.dependency.Kotlin
    configurations.all {
        resolutionStrategy {
            force(
                Kotlin.stdLib,
                Kotlin.stdLibCommon
            )
        }
    }
}

repositories.applyStandard()

plugins {
    `java-library`
    kotlin("jvm") version io.spine.internal.dependency.Kotlin.version
    idea
    io.spine.internal.dependency.Protobuf.GradlePlugin.apply {
        id(id) version version
    }
    @Suppress("RemoveRedundantQualifierName")
    io.spine.internal.dependency.ErrorProne.GradlePlugin.apply {
        id(id)
    }
}

spinePublishing {
    with(PublishingRepos) {
        targetRepositories.addAll(
            cloudRepo,
            cloudArtifactRegistry,
            gitHub("gcloud-java")
        )
    }
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
        plugin("java-library")
        plugin("kotlin")
        plugin("jacoco")
        plugin("idea")
    }

    val javaVersion = JavaVersion.VERSION_11

    java {
        sourceCompatibility = javaVersion
        targetCompatibility = javaVersion
    }

    kotlin {
        explicitApi()
    }

    tasks.withType<KotlinCompile>().configureEach {
        kotlinOptions {
            jvmTarget = javaVersion.toString()
            freeCompilerArgs = listOf("-Xskip-prerelease-check")
        }
    }

    group = "io.spine.gcloud"
    version = extra["versionToPublish"]!!

    repositories.applyStandard()
}

subprojects {

    apply {
        plugin("com.google.protobuf")
        plugin("net.ltgt.errorprone")
        plugin("io.spine.mc-java")
        plugin("pmd")
        plugin("maven-publish")
        plugin("pmd-settings")
    }

    tasks {
        withType<JavaCompile> {
            configureJavac()
            configureErrorProne()
        }
        withType<Test> {
            configureLogging()
        }
        registerTestTasks()
    }
    LicenseReporter.generateReportIn(project)
    JavadocConfig.applyTo(project)
    CheckStyleConfig.applyTo(project)

    // Required to fetch `androidx.annotation:annotation:1.1.0`,
    // which is a transitive dependency of `com.google.cloud:google-cloud-datastore`.
    repositories {
        google()
    }

    configurations.forceVersions()
    configurations {
        all {
            resolutionStrategy {
                force(
                    Apache.httpCore,
                    Apache.codec,

                    Grpc.api,
                    Grpc.auth,
                    Grpc.core,
                    Grpc.context,
                    Grpc.stub,
                    Grpc.protobuf,
                    Grpc.protobufLite,

                    PerfMark.api,

                    GoogleApis.AuthLibrary.credentials,
                    GoogleApis.commonProtos,

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
        }

        implementation("io.spine:spine-server:$spineCoreVersion")

        testImplementation(JUnit.runner)
        testImplementation("io.spine.tools:spine-testutil-server:$spineCoreVersion")
        testImplementation(
            group = "io.spine",
            name = "spine-server",
            version = spineCoreVersion,
            classifier = "test"
        )
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
            generatedSourceDirs.addAll(
                files(
                    generatedJavaDir,
                    generatedGrpcDir,
                    generatedSpineDir,
                    generatedTestJavaDir,
                    generatedTestGrpcDir,
                    generatedTestSpineDir
                )
            )
            testSourceDirs.add(file(generatedTestJavaDir))

            isDownloadJavadoc = true
            isDownloadSources = true
        }
    }

    updateGitHubPages(spineBaseVersion) {
        allowInternalJavadoc.set(true)
        rootFolder.set(rootDir)
    }
    project.tasks["publish"].dependsOn("${project.path}:updateGitHubPages")
}

JacocoConfig.applyTo(project)
PomGenerator.applyTo(project)
LicenseReporter.mergeAllReports(project)
