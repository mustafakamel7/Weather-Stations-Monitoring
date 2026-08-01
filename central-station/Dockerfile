FROM eclipse-temurin:21-jre-jammy

WORKDIR /app
COPY target/central-station-1.0-SNAPSHOT.jar app.jar

ENV KAFKA_BOOTSTRAP=kafka:29092

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
