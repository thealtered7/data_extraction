plugins {
    java
    eclipse
    id("org.springframework.boot") version "3.5.15"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.dataextraction"
version = "0.1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.shell:spring-shell-dependencies:3.4.3")
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.shell:spring-shell-starter")

    implementation("io.micrometer:micrometer-registry-otlp")
    implementation("io.micrometer:micrometer-tracing-bridge-otel")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp")
    implementation("io.opentelemetry:opentelemetry-sdk")
    implementation("org.postgresql:postgresql")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("org.apache.kafka:kafka-clients")
    implementation("io.confluent:kafka-avro-serializer:7.9.10") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation("io.confluent:kafka-connect-avro-converter:7.9.10") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation("io.confluent:kafka-json-schema-serializer:7.9.10") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation("software.amazon.glue:schema-registry-serde:2.0.1") {
        // Confluent JSON Schema needs io.yokota's JsonSchemaConfig.builder().
        // Glue also pulls the older Scala artifact, which occupies the same package and wins at runtime.
        exclude(group = "com.kjetland", module = "mbknor-jackson-jsonschema_2.13")
    }
    implementation("io.debezium:debezium-api:3.5.0.Final")
    implementation("io.debezium:debezium-embedded:3.5.0.Final")
    implementation("io.debezium:debezium-connector-postgres:3.5.0.Final")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.test {
    useJUnitPlatform()
}
