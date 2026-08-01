package com.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class OpenMeteoAdapter {

    // Station ID 11 = Open-Meteo virtual station
    private static final long   STATION_ID      = 11L;
    private static final String KAFKA_BOOTSTRAP =
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
    private static final String TOPIC           = "weather";
    private static final String DEAD_LETTER_TOPIC = "dead-letter"; // Dead Letter Channel

    // Cairo, Egypt coordinates
    private static final String LATITUDE  =
        System.getenv().getOrDefault("LATITUDE", "30.0444");
    private static final String LONGITUDE =
        System.getenv().getOrDefault("LONGITUDE", "31.2357");

    private static final String API_URL =
        "https://api.open-meteo.com/v1/forecast" +
        "?latitude=" + LATITUDE +
        "&longitude=" + LONGITUDE +
        "&current=temperature_2m,relative_humidity_2m,wind_speed_10m" +
        "&wind_speed_unit=kmh" +
        "&temperature_unit=fahrenheit";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Idempotent receiver
    private static long s_no = 1;
    private static String lastPayloadHash = "";

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Open-Meteo Channel Adapter...");
        System.out.println("Location: lat=" + LATITUDE + " lon=" + LONGITUDE);
        System.out.println("Kafka: " + KAFKA_BOOTSTRAP);
        System.out.println("Virtual Station ID: " + STATION_ID);

        // Kafka Producer setup
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  KAFKA_BOOTSTRAP);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG,    "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // POLLING CONSUMER — poll API every 1 second
            while (true) {
                try {
                    // 1. Poll Open-Meteo API
                    String rawResponse = fetchFromApi(httpClient);
                    if (rawResponse == null) {
                        Thread.sleep(1000);
                        continue;
                    }

                    // 2. EIP: IDEMPOTENT RECEIVER
                    String hash = String.valueOf(rawResponse.hashCode());
                    if (hash.equals(lastPayloadHash)) {
                        System.out.println("[Adapter] Duplicate response — skipping (Idempotent)");
                        s_no++; // still increment s_no
                        Thread.sleep(1000);
                        continue;
                    }
                    lastPayloadHash = hash;

                    // 3. EIP: ENVELOPE WRAPPER(wrap raw API response in our schema)
                    String message = transformToWeatherStatus(rawResponse);
                    if (message == null) {
                        // INVALID MESSAGE CHANNEL(send malformed messages to dead-letter topic instead of dropping)
                        sendToDeadLetter(producer, rawResponse,
                                        "Failed to transform Open-Meteo response");
                        s_no++;
                        Thread.sleep(1000);
                        continue;
                    }

                    // 4. Send to weather topic
                    ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, String.valueOf(STATION_ID), message);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("[Adapter] Send failed: " + exception.getMessage());
                        } else {
                            System.out.println("[Adapter] Sent s_no=" + s_no
                                + " → partition=" + metadata.partition()
                                + " offset=" + metadata.offset());
                        }
                    });

                    s_no++;

                } catch (Exception e) {
                    System.err.println("[Adapter] Error: " + e.getMessage());
                    // EIP: DEAD LETTER CHANNEL — send failed messages to dead-letter topic
                    sendToDeadLetter(producer, e.getMessage(), "Unexpected adapter error");
                }

                Thread.sleep(1000); // Poll every 1 second
            }
        }
    }

    /**
     * EIP Pattern: CHANNEL ADAPTER
     * Converts Open-Meteo API response → internal WeatherStatus schema
     */
    private static String transformToWeatherStatus(String rawResponse) {
        try {
            JsonNode root = MAPPER.readTree(rawResponse);
            JsonNode current = root.path("current");

            if (current.isMissingNode()) return null;

            // Extract fields from Open-Meteo response
            double tempF     = current.path("temperature_2m").asDouble();
            int    humidity  = current.path("relative_humidity_2m").asInt();
            double windSpeed = current.path("wind_speed_10m").asDouble();
            long   timestamp = System.currentTimeMillis() / 1000L;

            // EIP: ENVELOPE WRAPPER — wrap in our standard WeatherStatus format
            ObjectNode weather = MAPPER.createObjectNode();
            weather.put("humidity",    humidity);
            weather.put("temperature", (int) tempF);
            weather.put("wind_speed",  (int) windSpeed);

            ObjectNode message = MAPPER.createObjectNode();
            message.put("station_id",        STATION_ID);
            message.put("s_no",              s_no);
            message.put("battery_status",    "high"); // API source = always "high"
            message.put("status_timestamp",  timestamp);
            message.put("source",            "open-meteo"); // extra field to identify source
            message.set("weather",           weather);

            return MAPPER.writeValueAsString(message);

        } catch (Exception e) {
            System.err.println("[Adapter] Transform error: " + e.getMessage());
            return null;
        }
    }

    /**
     * Calls the Open-Meteo REST API and returns raw JSON string
     */
    private static String fetchFromApi(CloseableHttpClient client) {
        try {
            HttpGet request = new HttpGet(API_URL);
            return client.execute(request, response -> {
                int status = response.getCode();
                if (status != 200) {
                    System.err.println("[Adapter] API returned status: " + status);
                    return null;
                }
                return EntityUtils.toString(response.getEntity());
            });
        } catch (Exception e) {
            System.err.println("[Adapter] API call failed: " + e.getMessage());
            return null;
        }
    }

    /**
     * EIP Pattern: DEAD LETTER CHANNEL
     * Sends failed/invalid messages to a separate topic instead of dropping them
     */
    private static void sendToDeadLetter(KafkaProducer<String, String> producer,
                                          String payload, String reason) {
        try {
            ObjectNode deadLetter = MAPPER.createObjectNode();
            deadLetter.put("reason",    reason);
            deadLetter.put("timestamp", System.currentTimeMillis() / 1000L);
            deadLetter.put("source",    "open-meteo-adapter");
            deadLetter.put("payload",   payload);

            String json = MAPPER.writeValueAsString(deadLetter);
            producer.send(new ProducerRecord<>(DEAD_LETTER_TOPIC, json));
            System.out.println("[Dead Letter] Sent failed message to dead-letter topic");
        } catch (Exception e) {
            System.err.println("[Dead Letter] Failed to send to dead-letter: " + e.getMessage());
        }
    }
}
