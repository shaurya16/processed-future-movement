FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY ingestion-service/pom.xml ingestion-service/pom.xml
COPY processing-service/pom.xml processing-service/pom.xml
COPY common/src common/src
COPY ingestion-service/src ingestion-service/src
RUN mvn -B -pl ingestion-service -am -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/ingestion-service/target/ingestion-service-0.1.0-SNAPSHOT-exec.jar app.jar
COPY sample-data/ sample-data/
ENTRYPOINT ["java", "-jar", "app.jar"]
