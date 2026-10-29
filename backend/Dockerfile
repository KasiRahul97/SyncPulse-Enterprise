# syntax=docker/dockerfile:1
# ---- Stage 1: build (Maven + JDK 21) ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Cache dependencies first.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

# Build. Unit tests run in CI / `mvn test`; the image build skips them for a
# fast, deterministic runtime artifact (integration tests need a Docker daemon).
COPY src ./src
RUN mvn -B -q clean package -DskipTests

# ---- Stage 2: runtime (JRE 21) ----
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

# curl for the container healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 1001 syncpulse
COPY --from=build /app/target/syncpulse-backend.jar app.jar
USER syncpulse

EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
