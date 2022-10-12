plugins {
    `java-library`
}

val kafkaVersion : String by extra
val spotBugsVersion : String by extra
val hamcrestVersion : String by extra
val log4jVersion : String by extra
val testcontainersVersion : String by extra

dependencies {

    api("org.hamcrest:hamcrest-all:$hamcrestVersion")

    implementation("com.hashicorp:cdktf:0.12.2");
    implementation("software.constructs:constructs:10.0.15");
    implementation("com.google.guava:guava:28.0-jre");
    implementation("com.microsoft.terraform:terraform-client:1.0.0");

    implementation("org.apache.kafka:kafka-clients:${kafkaVersion}");


    implementation("org.projectlombok:lombok:1.18.22")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.4")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.13.4")
    implementation("com.github.spotbugs:spotbugs-annotations:$spotBugsVersion")


    testImplementation("org.testcontainers:testcontainers:${testcontainersVersion}")
    testImplementation("org.testcontainers:junit-jupiter:${testcontainersVersion}")
    testImplementation("org.testcontainers:kafka:${testcontainersVersion}")

    testImplementation("org.apache.logging.log4j:log4j-api:${log4jVersion}")
    testImplementation("org.apache.logging.log4j:log4j-core:${log4jVersion}")
    testImplementation("org.apache.logging.log4j:log4j-slf4j18-impl:${log4jVersion}")
}