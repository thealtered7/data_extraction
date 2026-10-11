# --- build ---
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /build

COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle/wrapper/ gradle/wrapper/
COPY src/ src/

RUN chmod +x gradlew \
    && ./gradlew bootJar --no-daemon -x test \
    && find build/libs -name '*.jar' ! -name '*-plain.jar' -exec cp {} /build/app.jar \;

# --- runtime ---
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

COPY --from=build /build/app.jar /app/app.jar

VOLUME ["/app/output"]

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
