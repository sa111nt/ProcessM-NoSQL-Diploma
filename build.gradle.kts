import org.gradle.api.plugins.antlr.AntlrTask

plugins {
    kotlin("jvm") version "2.1.10"
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
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

tasks.withType<AntlrTask> {
    outputDirectory = file("build/generated-src/antlr/main")
    arguments = listOf("-visitor")
}

sourceSets {
    named("main") {
        java.srcDir("build/generated-src/antlr/main")
    }
}

tasks.named("compileKotlin") {
    dependsOn("generateGrammarSource")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(23)
}