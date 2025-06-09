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
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

val kafkaVersion : String by extra
val spotBugsVersion : String by extra
val jacksonVersion : String by extra
val lombokVersion : String by extra
val confluentVersion : String by extra


dependencies {
    api("com.github.spotbugs:spotbugs-annotations:$spotBugsVersion")
    api("io.confluent:kafka-schema-registry-client:$confluentVersion")
    api("io.confluent:kafka-json-schema-provider:$confluentVersion")
    api("io.confluent:kafka-avro-serializer:$confluentVersion")
    api("io.confluent:kafka-json-schema-serializer:$confluentVersion")
    api("io.confluent:kafka-protobuf-provider:$confluentVersion")
    api("io.confluent:kafka-protobuf-serializer:$confluentVersion")
    api("io.confluent:kafka-streams-protobuf-serde:$confluentVersion")
    api("io.confluent:kafka-streams-avro-serde:$confluentVersion")
    api("com.google.protobuf:protobuf-java:3.25.3")

    api(project(":parser"))

    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("commons-io:commons-io:2.19.0")
    implementation("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")

    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testCompileOnly("org.projectlombok:lombok:$lombokVersion")
    testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testRuntimeOnly("commons-codec:commons-codec:1.18.0")

    testImplementation(project(":kafka-test"))
}