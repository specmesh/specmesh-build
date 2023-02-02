/*
 * Copyright 2023 SpecMesh Contributors (https://github.com/specmesh)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    java
    `maven-publish`
    signing
    id("com.github.spotbugs") version "5.0.13"
    id("com.diffplug.spotless") version "6.11.0"
    id("pl.allegro.tech.build.axion-release") version "1.14.3"
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
}

project.version = scmVersion.version
project.group = "io.specmesh"

allprojects {
    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "checkstyle")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")

    tasks.jar {
        onlyIf { sourceSets.main.get().allSource.files.isNotEmpty() }
    }
}

subprojects {
    project.version = project.parent?.version!!

    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    repositories {
        google()
        mavenCentral()
        maven {
            url = uri("https://packages.confluent.io/maven/")
        }
        maven {
            url = uri("https://repository.mulesoft.org/nexus/content/repositories/public/")
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11

        withSourcesJar()
        withJavadocJar()
    }

    extra.apply {
        set("kafkaVersion", "3.3.1")
        set("openTracingVersion", "0.33.0")
        set("observabilityVersion", "1.1.8")
        set("guavaVersion", "31.1-jre")
        set("confluentVersion", "7.3.1")
        set("jacksonVersion", "2.14.2")
        set("medeiaValidatorVersion", "1.1.0")
        set("junitVersion", "5.9.2")
        set("mockitoVersion", "5.1.1")
        set("junitPioneerVersion", "1.9.1")
        set("spotBugsVersion", "4.7.3")
        set("hamcrestVersion", "1.3")
        set("log4jVersion", "2.18.0")
        set("classGraphVersion", "4.8.21")
        set("testcontainersVersion", "1.17.6")
        set("lombokVersion", "1.18.24")
    }

    val junitVersion: String by extra
    val jacksonVersion: String by extra
    val mockitoVersion: String by extra
    val junitPioneerVersion: String by extra
    val guavaVersion : String by extra
    val hamcrestVersion : String by extra
    val log4jVersion : String by extra

    dependencies {
        testImplementation(project(":parser"))
        testImplementation(project(":kafka"))
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
        testImplementation("org.junit-pioneer:junit-pioneer:$junitPioneerVersion")
        testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion")
        testImplementation("org.hamcrest:hamcrest-all:$hamcrestVersion")
        testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
        testImplementation("com.google.guava:guava-testlib:$guavaVersion")
        testImplementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
        testImplementation("org.apache.logging.log4j:log4j-slf4j18-impl:$log4jVersion")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    }

    tasks.compileJava {
        options.compilerArgs.add("-Xlint:all,-serial,-processing")
        options.compilerArgs.add("-Werror")
    }

    tasks.test {
        useJUnitPlatform()
        setForkEvery(1)
        maxParallelForks = 2
        testLogging {
            showStandardStreams = true
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showCauses = true
            showExceptions = true
            showStackTraces = true
        }
    }

    tasks.javadoc {
        if (JavaVersion.current().isJava9Compatible) {
            (options as StandardJavadocDocletOptions).apply {
                addBooleanOption("html5", true)
                // Why -quite? See: https://github.com/gradle/gradle/issues/2354
                addStringOption("Xwerror", "-quiet")
            }
        }
    }

    spotless {
        java {
            googleJavaFormat("1.15.0").aosp()
            indentWithSpaces()
            importOrder()
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    tasks.register("format") {
        dependsOn("spotlessCheck", "spotlessApply")
    }

    tasks.register("checkstyle") {
        dependsOn("checkstyleMain", "checkstyleTest")
    }

    tasks.register("spotbugs") {
        dependsOn("spotbugsMain", "spotbugsTest")
    }

    tasks.register("static") {
        dependsOn("checkstyle", "spotbugs")
    }

    spotbugs {
        tasks.spotbugsMain {
            reports.create("html") {
                isEnabled = true
                setStylesheet("fancy-hist.xsl")
            }
        }
        tasks.spotbugsTest {
            reports.create("html") {
                isEnabled = true
                setStylesheet("fancy-hist.xsl")
            }
        }
    }

    tasks.jar {
        archiveBaseName.set("specmesh-${project.name}")
    }

    publishing {
        repositories {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/specmesh/${rootProject.name}")
                credentials {
                    username = System.getenv("GITHUB_ACTOR")
                    password = System.getenv("GITHUB_TOKEN")
                }
            }
        }

        publications {
            create<MavenPublication>("mavenArtifacts") {
                from(components["java"])

                artifactId = "specmesh-${artifactId}"
                project.group = "io.specmesh"

                pom {
                    name.set("${project.group}:${artifactId}")

                    description.set("Specmesh ${project.name.capitalize()} library".replace("-", " "))

                    url.set("https://www.specmesh.io")

                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }

                    developers {
                        developer {
                            name.set("Neil Avery")
                            email.set("8012398+neil-avery@users.noreply.github.com")
                            organization.set("SpecMesh Master Builders")
                            organizationUrl.set("https://www.specmesh.io")
                        }

                        developer {
                            name.set("Andy Coates")
                            email.set("8012398+big-andy-coates@users.noreply.github.com")
                            organization.set("SpecMesh Master Builders")
                            organizationUrl.set("https://www.specmesh.io")
                        }
                    }

                    scm {
                        connection.set("scm:git:git://github.com/specmesh/${rootProject.name}.git")
                        developerConnection.set("scm:git:ssh://github.com/specmesh/${rootProject.name}.git")
                        url.set("https://github.com/specmesh/${rootProject.name}")
                    }

                    issueManagement {
                        url.set("https://github.com/specmesh/${rootProject.name}/issues")
                    }
                }
            }
        }
    }

    signing {
        setRequired {
           !project.version.toString().endsWith("-SNAPSHOT")
                    && !project.hasProperty("skipSigning")
        }

        if (project.hasProperty("signingKey")) {
            useInMemoryPgpKeys(properties["signingKey"].toString(), properties["signingPassword"].toString())
        }

        sign(publishing.publications["mavenArtifacts"])
    }
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))

            if (project.hasProperty("SONA_USERNAME")) {
                username.set(project.property("SONA_USERNAME").toString())
            }

            if (project.hasProperty("SONA_PASSWORD")) {
                password.set(project.property("SONA_PASSWORD").toString())
            }
        }
    }
}

defaultTasks("format", "static", "check")