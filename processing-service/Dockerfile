FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY ingestion-service/pom.xml ingestion-service/pom.xml
COPY processing-service/pom.xml processing-service/pom.xml
COPY common/src common/src
COPY ingestion-service/src ingestion-service/src
COPY processing-service/src processing-service/src
RUN mvn -B -pl processing-service -am -DskipTests package

# Deliberately NOT -alpine, unlike ingestion-service: Kafka Streams' RocksDB state
# store loads a JNI library that needs glibc, and musl gives a fatal UnsatisfiedLinkError.
# It surfaces only once Streams reaches a real broker and gets partitions assigned, so no
# unit test or container smoke test catches it. See docs/superpowers/specs/2026-08-11-k8s-design.md.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/processing-service/target/processing-service-0.1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
