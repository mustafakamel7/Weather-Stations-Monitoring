package com.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class WeatherStation {

    private static final long STATION_ID =
        Long.parseLong(System.getenv().getOrDefault("STATION_ID", "1"));

    private static final String KAFKA_BOOTSTRAP =
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "weather-project-kafka-1:9092");

    private static final String TOPIC = "weather";
    private static final Random RANDOM = new Random();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Weather Station ID: " + STATION_ID);
        System.out.println("Connecting to Kafka at: " + KAFKA_BOOTSTRAP);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long s_no = 1;

            while (true) {
                // 10% message drop
                if (RANDOM.nextDouble() < 0.10) {
                    System.out.println("[Station " + STATION_ID + "] Dropping message #" + s_no);
                    s_no++;
                    Thread.sleep(1000);
                    continue;
                }

                WeatherStatus status = new WeatherStatus(
                    STATION_ID, s_no, randomBattery(), randomWeather()
                );

                String json = MAPPER.writeValueAsString(status);

                ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, String.valueOf(STATION_ID), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Failed to send: " + exception.getMessage());
                        // Send to dead-letter topic instead of dropping
                        try {
                            String deadLetterMsg = "{\"reason\":\"send_failure\","
                                + "\"station_id\":" + STATION_ID
                                + ",\"s_no\":" + status.s_no
                                + ",\"error\":\"" + exception.getMessage() + "\"}";
                            producer.send(new ProducerRecord<>("dead-letter",
                                        String.valueOf(STATION_ID), deadLetterMsg));
                            System.out.println("[Dead Letter] Message #" + status.s_no
                                + " sent to dead-letter topic");
                        } catch (Exception e) {
                            System.err.println("Dead letter send also failed: " + e.getMessage());
                        }
                    } else {
                        System.out.println("[Station " + STATION_ID
                            + "] Sent #" + status.s_no
                            + " → partition=" + metadata.partition()
                            + " offset=" + metadata.offset());
                    }
                });

                s_no++;
                Thread.sleep(1000);
            }
        }
    }

    // low=30%, medium=40%, high=30%
    private static String randomBattery() {
        double r = RANDOM.nextDouble();
        if (r < 0.30) return "low";
        if (r < 0.70) return "medium";
        return "high";
    }

    private static Weather randomWeather() {
        return new Weather(
            RANDOM.nextInt(101),
            60 + RANDOM.nextInt(61),
            RANDOM.nextInt(101)
        );
    }
}
