plugins {
    `java-library`
}

val kafkaVersion : String by extra
val spotBugsVersion : String by extra
val hamcrestVersion : String by extra
val log4jVersion : String by extra

dependencies {
    api("org.hamcrest:hamcrest-all:$hamcrestVersion")
    implementation("com.github.spotbugs:spotbugs-annotations:$spotBugsVersion")
//    implementation(project(":common"))
    implementation("org.projectlombok:lombok:1.18.22")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.4")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.13.4")


    testImplementation("org.apache.logging.log4j:log4j-api:${log4jVersion}")
    testImplementation("org.apache.logging.log4j:log4j-core:${log4jVersion}")
    testImplementation("org.apache.logging.log4j:log4j-slf4j18-impl:${log4jVersion}")
}