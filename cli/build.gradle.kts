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

import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import com.bmuschko.gradle.docker.tasks.image.DockerPushImage

plugins {
    application
    id("com.bmuschko.docker-remote-api")
}

val kafkaVersion : String by extra
val spotBugsVersion : String by extra
val jacksonVersion : String by extra
val lombokVersion : String by extra
val confluentVersion : String by extra
val testcontainersVersion : String by extra
val log4jVersion : String by extra


dependencies {
    implementation("info.picocli:picocli:4.7.5")
    implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:$log4jVersion")

    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")



    implementation(project(":parser"))
    implementation(project(":kafka"))
    testImplementation(project(":kafka-test"))

    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:kafka:$testcontainersVersion")

    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testCompileOnly("org.projectlombok:lombok:$lombokVersion")
    testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")


}
application {
    mainModule.set("io.specmesh.cli")
    mainClass.set("io.specmesh.cli.Main")
}
val buildAppImage = tasks.register<DockerBuildImage>("buildAppImage") {
    dependsOn("prepareDocker")
    buildArgs.put("APP_NAME", project.name)
    buildArgs.put("APP_VERSION", "${project.version}")
    images.add("ghcr.io/specmesh/${rootProject.name}-${project.name}:latest")
    images.add("ghcr.io/specmesh/${rootProject.name}-${project.name}:${project.version}")
}

tasks.register<Copy>("prepareDocker") {
    dependsOn("distTar")

    from(
        layout.projectDirectory.file("Dockerfile"),
        tarTree(layout.buildDirectory.file("distributions/${project.name}-${project.version}.tar")),
        layout.projectDirectory.dir("include"),
    )

    into(buildAppImage.get().inputDir)
}


tasks.register<DockerPushImage>("pushAppImage") {
    dependsOn("buildAppImage")
//    if (!snapshot) {
    images.add("ghcr.io/specmesh/${rootProject.name}-${project.name}:latest")
//    }
    images.add("ghcr.io/specmesh/${rootProject.name}-${project.name}:${project.version}")
}