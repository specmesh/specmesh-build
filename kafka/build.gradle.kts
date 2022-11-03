plugins {
    `java-library`
}

val kafkaVersion : String by extra
val spotBugsVersion : String by extra
val hamcrestVersion : String by extra
val log4jVersion : String by extra
val testcontainersVersion : String by extra
val lombokVersion : String by extra

dependencies {

    implementation(project(":parser"));

    api("org.hamcrest:hamcrest-all:$hamcrestVersion")

    implementation("com.hashicorp:cdktf:0.12.2");
    implementation("software.constructs:constructs:10.0.15");
    implementation("com.google.guava:guava:28.0-jre");
    implementation("com.microsoft.terraform:terraform-client:1.0.0");

    implementation("org.apache.kafka:kafka-clients:${kafkaVersion}");


    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")

    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("org.slf4j:slf4j-simple:1.7.36")

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