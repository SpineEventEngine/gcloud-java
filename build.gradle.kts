/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import io.spine.gradle.internal.DependencyResolution
import io.spine.gradle.internal.Deps
import io.spine.gradle.internal.PublishingRepos

buildscript {

    apply(from = "$rootDir/version.gradle.kts")

    @Suppress("RemoveRedundantQualifierName") // Cannot use imports here.
    val resolution = io.spine.gradle.internal.DependencyResolution
    @Suppress("RemoveRedundantQualifierName") // Cannot use imports here.
    val deps = io.spine.gradle.internal.Deps

    resolution.defaultRepositories(repositories)

    val spineBaseVersion: String by extra

    dependencies {
        classpath(deps.build.gradlePlugins.errorProne) {
            exclude(group = "com.google.guava")
        }
        classpath(deps.build.gradlePlugins.protobuf)
        classpath("io.spine.tools:spine-model-compiler:$spineBaseVersion")
    }
}

plugins {
    `java-library`
    idea
    jacoco
    @Suppress("RemoveRedundantQualifierName") // Cannot use imports here.
    id("com.google.protobuf").version(io.spine.gradle.internal.Deps.versions.protobufPlugin)
    @Suppress("RemoveRedundantQualifierName") // Cannot use imports here.
    id("net.ltgt.errorprone").version(io.spine.gradle.internal.Deps.versions.errorPronePlugin)
}

allprojects {
    apply(from = "$rootDir/version.gradle.kts")
    apply(from = "$rootDir/config/gradle/dependencies.gradle")

    group = "io.spine.gcloud"
    version = extra["versionToPublish"]!!
}

extra["credentialsPropertyFile"] = PublishingRepos.cloudRepo.credentials
extra["projectsToPublish"] = listOf("datastore", "stackdriver-trace", "testutil-gcloud", "pubsub")

subprojects {
    apply {
        plugin("java-library")
        plugin("pmd")
        plugin("idea")
        plugin("maven-publish")
        plugin("net.ltgt.errorprone")
        plugin("com.google.protobuf")
        plugin("io.spine.tools.spine-model-compiler")
        from(Deps.scripts.modelCompiler(project))
        from(Deps.scripts.slowTests(project))
        from(Deps.scripts.testOutput(project))
        from(Deps.scripts.javadocOptions(project))
        from(Deps.scripts.javacArgs(project))
        from(Deps.scripts.projectLicenseReport(project))
        from(Deps.scripts.pmd(project))
    }

    DependencyResolution.excludeProtobufLite(configurations)
    configurations {
        all {
            resolutionStrategy {
                force(Deps.build.guava, Deps.test.guavaTestlib)
            }
        }
    }

    val sourcesRootDir = "$projectDir/src"
    val generatedRootDir = "$projectDir/generated"
    val generatedJavaDir = "$generatedRootDir/main/java"
    val generatedTestJavaDir = "$generatedRootDir/test/java"
    val generatedGrpcDir = "$generatedRootDir/main/grpc"
    val generatedTestGrpcDir = "$generatedRootDir/test/grpc"
    val generatedSpineDir = "$generatedRootDir/main/spine"
    val generatedTestSpineDir = "$generatedRootDir/test/spine"
    val testArtifactsScript = "$rootDir/scripts/test-artifacts.gradle"
    val filterInternalJavadocsScript = "$rootDir/config/gradle/filter-internal-javadoc.gradle"
    val updateDocsPlugin = "$rootDir/scripts/update-gh-pages.gradle"

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    DependencyResolution.defaultRepositories(repositories)

    // Required to fetch `androidx.annotation:annotation:1.1.0`,
    // which is a transitive dependency of `com.google.cloud:google-cloud-datastore`.
    repositories {
        google()
    }

    val spineCoreVersion: String by extra

    dependencies {
        errorprone(Deps.build.errorProneCore)
        errorproneJavac(Deps.build.errorProneJavac)

        implementation("io.spine:spine-server:$spineCoreVersion")

        compileOnlyApi(Deps.build.checkerAnnotations)
        compileOnlyApi(Deps.build.jsr305Annotations)
        Deps.build.errorProneAnnotations.forEach { compileOnlyApi(it) }

        testImplementation("io.spine:spine-testutil-server:$spineCoreVersion")
        testImplementation(group = "io.spine",
                           name = "spine-server",
                           version = spineCoreVersion,
                           classifier = "test")
        testImplementation(Deps.test.hamcrest)
        testImplementation(Deps.test.guavaTestlib)
        testImplementation(Deps.test.junitPioneer)
        Deps.test.junit5Api.forEach { testImplementation(it) }
        Deps.test.truth.forEach { testImplementation(it) }
        testRuntimeOnly(Deps.test.junit5Runner)
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
}

apply {
    from(Deps.scripts.jacoco(project))
    from(Deps.scripts.publish(project))
    from(Deps.scripts.repoLicenseReport(project))
    from(Deps.scripts.generatePom(project))
}
