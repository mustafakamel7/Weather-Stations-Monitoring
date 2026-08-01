package com.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class RainDetector {

    private static final String KAFKA_BOOTSTRAP =
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

    private static final String INPUT_TOPIC  = "weather";
    private static final String OUTPUT_TOPIC = "rain-alerts";
    private static final int    HUMIDITY_THRESHOLD = 70;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        System.out.println("Starting Rain Detector...");
        System.out.println("Connecting to Kafka at: " + KAFKA_BOOTSTRAP);
        System.out.println("Monitoring topic: " + INPUT_TOPIC);
        System.out.println("Alerts will go to: " + OUTPUT_TOPIC);
        System.out.println("Humidity threshold: " + HUMIDITY_THRESHOLD + "%");

        // --- Kafka Streams config ---
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "rain-detector-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());

        // --- Stream topology ---  (the DSL entry point)
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> weatherStream = builder.stream(INPUT_TOPIC);

        weatherStream
            // 1. Filter: only messages where humidity > 70
            .filter((key, value) -> {
                try {
                    JsonNode node = MAPPER.readTree(value);
                    int humidity = node.path("weather").path("humidity").asInt();
                    return humidity > HUMIDITY_THRESHOLD;
                } catch (Exception e) {
                    System.err.println("Failed to parse message: " + e.getMessage());
                    return false; 
                }
            })
            // 2. Transform: add a rain_alert flag and alert message
            // DSL operation: .mapValues()
            // Takes each passing message and returns a NEW modified version of it
            .mapValues(value -> {
                try {
                    ObjectNode node = (ObjectNode) MAPPER.readTree(value);
                    int humidity = node.path("weather").path("humidity").asInt();
                    long stationId = node.path("station_id").asLong();

                    node.put("rain_alert", true);
                    node.put("alert_message",
                        "RAIN ALERT: Station " + stationId +
                        " reports humidity at " + humidity + "%");

                    System.out.println("[Rain Detector] " +
                        node.get("alert_message").asText());

                    return MAPPER.writeValueAsString(node);
                } catch (Exception e) {
                    return value; // return original if transform fails
                }
            })
            // 3. Forward to rain-alerts topic
            .to(OUTPUT_TOPIC);

        // --- Start the stream ---
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
        System.out.println("Rain Detector is running.");
    }
}
