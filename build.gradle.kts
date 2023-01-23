tasks.wrapper {
    gradleVersion = "7.4"
    // You can either download the binary-only version of Gradle (BIN) or
    // the full version (with sources and documentation) of Gradle (ALL)
    distributionType = Wrapper.DistributionType.ALL
}
plugins {
    java
    `maven-publish`
    `signing`
    id("com.github.spotbugs") version "4.7.2"
    id("com.diffplug.spotless") version "6.11.0"
    id("pl.allegro.tech.build.axion-release") version "1.11.0"
}


//project.version = scmVersion.version
project.group = "io.specmesh"
project.version = "0.1.0-SNAPSHOT"

allprojects {
    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "signing")
    apply(plugin = "checkstyle")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")
}

subprojects {
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
    apply(plugin = "maven-publish")

    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "checkstyle")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")


    java {

        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    project.version = project.parent?.version!!

    extra.apply {
        set("kafkaVersion", "3.3.1")
        set("openTracingVersion", "0.33.0")
        set("observabilityVersion", "1.1.8")
        set("guavaVersion", "29.0-jre")
        set("confluentVersion", "7.2.2")
        set("jacksonVersion", "2.11.3")
        set("medeiaValidatorVersion", "1.1.0")
        set("junitVersion", "5.7.0")
        set("mockitoVersion", "3.4.6")
        set("junitPioneerVersion", "0.9.0")
        set("spotBugsVersion", "4.2.0")
        set("hamcrestVersion", "1.3")
        set("log4jVersion", "2.18.0")
        set("classGraphVersion", "4.8.21")
        set("testcontainersVersion", "1.17.3")
        set("lombokVersion", "1.18.24")
    }

    val junitVersion: String by extra
    val jacksonVersion: String by extra
    val mockitoVersion: String by extra
    val junitPioneerVersion: String by extra
    val guavaVersion : String by extra
    val hamcrestVersion : String by extra
    val log4jVersion : String by extra
    val testcontainersVersion : String by extra
    val lombokVersion : String by extra
    val confluentVersion : String by extra

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
        maxParallelForks = 2
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
            eclipse()
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
        setGroup("${project.group}")
        archiveVersion.set("${project.version}")
        archiveBaseName.set("${project.name}")

    }

    publishing {

        println("---------------- PUBLISHING --------------")
        repositories {
            maven {
                name = "GitHubPackagesSpecMesh"
                url = uri("https://maven.pkg.github.com/specmesh/${rootProject.name}")
                // set ~/.gradle/gradle.properties vars: GitHubPackagesSpecMeshUsername and Password accordingly
                credentials(PasswordCredentials::class)
            }
        }

        publications {
            create<MavenPublication>("mavenArtifacts") {
                from(components["java"])

                artifactId = "${project.group}-${artifactId}"
                project.group = "io.specmesh"

                pom {
                    name.set("${project.group}:${artifactId}")

                    description.set("${project.name.capitalize()} library".replace("-", " "))

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
                    }

                    scm {
                        connection.set("scm:git:git://github.com/specmesh/${rootProject.name}.git")
                        developerConnection.set("scm:git:ssh://github.com/specmesh/${rootProject.name}.git")
                        url.set("https://github.com/specmesh/${rootProject.name}")
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