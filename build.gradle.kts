tasks.wrapper {
    gradleVersion = "7.4"
    // You can either download the binary-only version of Gradle (BIN) or
    // the full version (with sources and documentation) of Gradle (ALL)
    distributionType = Wrapper.DistributionType.ALL
}
plugins {
    java
    id("com.github.spotbugs") version "4.7.2"
    id("com.diffplug.spotless") version "6.11.0"
    id("pl.allegro.tech.build.axion-release") version "1.11.0"
}


project.version = scmVersion.version

allprojects {
    repositories {
        google()
        mavenCentral()
    }
    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "checkstyle")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")

    group = "com.specmesh.build"

    java {
        withSourcesJar()

        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
}

subprojects {
    apply(plugin = "maven-publish")

    project.version = project.parent?.version!!

    extra.apply {
        set("kafkaVersion", "2.6.0")
        set("openTracingVersion", "0.33.0")
        set("observabilityVersion", "1.1.8")
        set("guavaVersion", "29.0-jre")
        set("confluentVersion", "6.0.0")
        set("jacksonVersion", "2.11.3")
        set("medeiaValidatorVersion", "1.1.0")
        set("junitVersion", "5.7.0")
        set("mockitoVersion", "3.4.6")
        set("junitPioneerVersion", "0.9.0")
        set("spotBugsVersion", "4.2.0")
        set("hamcrestVersion", "1.3")
        set("log4jVersion", "2.14.0")
        set("classGraphVersion", "4.8.21")
        set("testcontainersVersion", "1.17.3")
    }

    val junitVersion: String by extra
    val jacksonVersion: String by extra
    val mockitoVersion: String by extra
    val junitPioneerVersion: String by extra
    val guavaVersion : String by extra
    val hamcrestVersion : String by extra
    val log4jVersion : String by extra
    val testcontainersVersion : String by extra

    dependencies {
        testImplementation(project(":parser"))
        testImplementation(project(":kafka"))
        testImplementation("org.junit.jupiter:junit-jupiter-api:${junitVersion}")
        testImplementation("org.junit.jupiter:junit-jupiter-params:${junitVersion}")
        testImplementation("org.junit-pioneer:junit-pioneer:${junitPioneerVersion}")
        testImplementation("org.mockito:mockito-junit-jupiter:${mockitoVersion}")
        testImplementation("org.hamcrest:hamcrest-all:$hamcrestVersion")
        testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${jacksonVersion}")
        testImplementation("com.google.guava:guava:${guavaVersion}")
        testImplementation("com.google.guava:guava-testlib:${guavaVersion}")
        testImplementation("org.apache.logging.log4j:log4j-api:${log4jVersion}")
        testImplementation("org.apache.logging.log4j:log4j-core:${log4jVersion}")
        testImplementation("org.apache.logging.log4j:log4j-slf4j18-impl:${log4jVersion}")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${junitVersion}")
    }

    tasks.compileJava {
        options.compilerArgs.add("-Xlint:all,-serial")
//        options.compilerArgs.add("-Werror")
    }

    tasks.test {
        useJUnitPlatform()
        setForkEvery(1)
        maxParallelForks = 4
        testLogging {
            showStandardStreams = true
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showCauses = true
            showExceptions = true
            showStackTraces = true
        }
    }

    spotless {
        java {
//            googleJavaFormat("1.9").aosp()
            indentWithSpaces()
            importOrder()
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    tasks.register("pre") {
        dependsOn("spotlessCheck", "spotlessApply")
    }

    tasks.register("preClean") {
        dependsOn("clean", "pre")
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

    configure<PublishingExtension> {
        publications {
            create<MavenPublication>("maven") {
                from(components["java"])
                artifactId = "specmesh-${project.name}"
            }
        }
    }
}