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
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")

    // Zależności do Mockito (Core 5.12.0 obsługuje inline, ale potrzebuje flag JVM)
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0")
}

// 1. Konfiguracja ANTLR
tasks.withType<AntlrTask> {
    maxHeapSize = "64m"

    // Generujemy pliki do folderu kończącego się na /ql, żeby zgadzało się z pakietem
    outputDirectory = file("build/generated-src/antlr/main/ql")

    // "-package", "ql" -> Dodaje "package ql;" do plików .java
    // "-Xexact-output-dir" -> Wymusza zapis w outputDirectory bez tworzenia podkatalogów z pakietu
    arguments = arguments + listOf("-visitor", "-listener", "-package", "ql", "-Xexact-output-dir")
}

// 2. Dodanie wygenerowanych źródeł do projektu
sourceSets {
    named("main") {
        java {
            srcDir("build/generated-src/antlr/main")
        }
    }
}

// 3. NAPRAWA BŁĘDU: Wymuszenie kolejności zadań
// Kompilacja Java musi czekać na wygenerowanie gramatyki (to naprawia błąd "Gradle detected a problem...")
tasks.withType<JavaCompile> {
    dependsOn("generateGrammarSource")
}

// Kompilacja Kotlin też musi czekać
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    dependsOn("generateGrammarSource")
}

tasks.test {
    useJUnitPlatform()
    // Flagi potrzebne dla Mockito na Java 23+
    jvmArgs("-Dnet.bytebuddy.experimental=true", "-Dorg.mockito.mock-maker-class=mock-maker-inline")
}

kotlin {
    jvmToolchain(23)
}