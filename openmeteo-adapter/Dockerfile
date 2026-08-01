FROM eclipse-temurin:21-jre-jammy

WORKDIR /app
COPY target/openmeteo-adapter-1.0-SNAPSHOT.jar app.jar

ENV KAFKA_BOOTSTRAP=kafka:29092
ENV LATITUDE=30.0444
ENV LONGITUDE=31.2357

ENTRYPOINT ["java", "-jar", "app.jar"]
