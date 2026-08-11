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

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/processing-service/target/processing-service-0.1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
