plugins {
    `java-library`
}

val kafkaVersion : String by extra
val spotBugsVersion : String by extra
val jacksonVersion : String by extra
val testcontainersVersion : String by extra
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
    api("com.google.protobuf:protobuf-java:3.21.11")

    implementation(project(":parser"))

    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("commons-io:commons-io:2.11.0")
    implementation("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")

    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testCompileOnly("org.projectlombok:lombok:$lombokVersion")
    testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")

    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:kafka:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
}