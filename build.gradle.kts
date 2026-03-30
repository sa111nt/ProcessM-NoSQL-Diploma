import org.gradle.api.plugins.antlr.AntlrTask

plugins {
    kotlin("jvm") version "2.1.10"

    kotlin("plugin.spring") version "2.1.10"
    id("org.springframework.boot") version "3.4.0"
    id("io.spring.dependency-management") version "1.1.6"

    id("antlr")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    antlr("org.antlr:antlr4:4.13.1")
    implementation("org.antlr:antlr4-runtime:4.13.1")

    testImplementation(kotlin("test"))
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.google.code.gson:gson:2.11.0")
    implementation("org.apache.commons:commons-compress:1.26.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    implementation("org.springframework.boot:spring-boot-starter-web")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")

    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0")
}

tasks.named<AntlrTask>("generateGrammarSource") {
    maxHeapSize = "64m"
    arguments = arguments + listOf("-visitor", "-listener", "-package", "ql", "-Xexact-output-dir")

    val outputDir = layout.buildDirectory.dir("generated-src/antlr/main/ql")
    outputDirectory = outputDir.get().asFile
}

sourceSets {
    main {
        java {
            srcDir(tasks.named("generateGrammarSource"))
        }
    }
    test {
        java {
            srcDir(tasks.named("generateTestGrammarSource"))
        }
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("-Dnet.bytebuddy.experimental=true", "-Dorg.mockito.mock-maker-class=mock-maker-inline")
    maxHeapSize = "8g"
}

kotlin {
    jvmToolchain(23)
}