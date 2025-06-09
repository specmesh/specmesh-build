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
    `java-library`
    id("com.google.protobuf") version "0.9.5"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

val testcontainersVersion : String by extra
val lombokVersion : String by extra
val junitVersion : String by extra
val protobufVersion : String by extra

dependencies {
    api(project(":parser"))
    api(project(":kafka"))
    api("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    api("org.testcontainers:testcontainers:$testcontainersVersion")
    implementation("org.testcontainers:kafka:$testcontainersVersion")

    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testCompileOnly("org.projectlombok:lombok:$lombokVersion")
    testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")

    testImplementation("com.google.protobuf:protobuf-java:$protobufVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testRuntimeOnly("commons-codec:commons-codec:1.18.0")
}