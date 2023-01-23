plugins {
    `java-library`
}

val spotBugsVersion : String by extra
val jacksonVersion : String by extra
val lombokVersion : String by extra

dependencies {
    api("com.github.spotbugs:spotbugs-annotations:$spotBugsVersion")
    api("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")

    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")

    compileOnly("org.projectlombok:lombok:$lombokVersion")
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testCompileOnly("org.projectlombok:lombok:$lombokVersion")
    testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")
}