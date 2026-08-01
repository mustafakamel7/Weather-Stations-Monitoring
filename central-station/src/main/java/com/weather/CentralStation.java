package com.weather;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

public class CentralStation {
    private static Bitcask bitcask;
    private static final List<String> parquetBatch = new ArrayList<>();
    private static final int BATCH_SIZE = 1000;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<Long, Long> lastProcessedSno = new ConcurrentHashMap<>();


    // Define the strict Avro schema for our Parquet files
    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"WeatherStatus\",\"fields\":[" +
                    "{\"name\":\"station_id\",\"type\":\"long\"}," +
                    "{\"name\":\"s_no\",\"type\":\"long\"}," +
                    "{\"name\":\"battery_status\",\"type\":\"string\"}," +
                    "{\"name\":\"status_timestamp\",\"type\":\"long\"}," +
                    "{\"name\":\"weather\",\"type\":{\"type\":\"record\",\"name\":\"Weather\",\"fields\":[" +
                    "{\"name\":\"humidity\",\"type\":\"int\"}," +
                    "{\"name\":\"temperature\",\"type\":\"int\"}," +
                    "{\"name\":\"wind_speed\",\"type\":\"int\"}" +
                    "]}}]}";

    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    public static void main(String[] args) throws Exception {
        // 1. Initialize Bitcask
        bitcask = new Bitcask(System.getenv().getOrDefault("BITCASK_DIR", "./data/bitcask"));

        // 2. Start HTTP Server for the bash client
        startHttpServer();

        // 3. Start Kafka Consumer
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("weather"));
            System.out.println("Central Station listening for weather data...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {

                    // IDEMPOTENT RECEIVER
                    try {
                        JsonNode node = MAPPER.readTree(record.value());
                        long stationId = node.get("station_id").asLong();
                        long sNo       = node.get("s_no").asLong();

                        Long lastSno = lastProcessedSno.get(stationId);
                        if (lastSno != null && sNo <= lastSno) {
                            System.out.println("[Idempotent] Skipping duplicate s_no="
                                + sNo + " for station " + stationId);
                            continue;
                        }
                        lastProcessedSno.put(stationId, sNo);
                    } catch (Exception e) {
                        System.err.println("Could not check idempotency: " + e.getMessage());
                    }

                    // A. Update Bitcask (Latest Status)
                    bitcask.put(record.key(), record.value());

                    // B. Add to Parquet Batch (Archiving)
                    parquetBatch.add(record.value());
                    if (parquetBatch.size() >= BATCH_SIZE) {
                        flushToParquet();
                    }
                }
            }
        }
    }

    private static void flushToParquet() {
        System.out.println("Flushing " + parquetBatch.size() + " records to Parquet...");
        if (parquetBatch.isEmpty()) return;

        // 1. Group records by partition path (Time & Station ID)
        Map<String, List<GenericRecord>> partitionedData = new HashMap<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:00");

        for (String json : parquetBatch) {
            try {
                JsonNode node = MAPPER.readTree(json);
                long stationId = node.get("station_id").asLong();

                // Convert timestamp to Date string (assuming seconds from origin)
                long timestamp = node.get("status_timestamp").asLong() * 1000L;
                String dateStr = dateFormat.format(new Date(timestamp));

                // Define the partition folder structure
                String partition = "time=" + dateStr + "/station_id=" + stationId;

                // 2. Map JSON to Avro GenericRecord
                GenericRecord record = new GenericData.Record(SCHEMA);
                record.put("station_id", stationId);
                record.put("s_no", node.get("s_no").asLong());
                record.put("battery_status", node.get("battery_status").asText());
                record.put("status_timestamp", node.get("status_timestamp").asLong());

                JsonNode weatherNode = node.get("weather");
                GenericRecord weatherRecord = new GenericData.Record(SCHEMA.getField("weather").schema());
                weatherRecord.put("humidity", weatherNode.get("humidity").asInt());
                weatherRecord.put("temperature", weatherNode.get("temperature").asInt());

                // Safely handle both the old schema (underscore) and new schema (space)
                int windSpeed = 0;
                if (weatherNode.has("wind speed")) {
                    windSpeed = weatherNode.get("wind speed").asInt();
                } else if (weatherNode.has("wind_speed")) {
                    windSpeed = weatherNode.get("wind_speed").asInt();
                }
                weatherRecord.put("wind_speed", windSpeed);

                record.put("weather", weatherRecord);

                // Add to the correct partition bucket
                partitionedData.computeIfAbsent(partition, k -> new ArrayList<>()).add(record);

            } catch (Exception e) {
                System.err.println("Error parsing JSON for Parquet: " + e.getMessage());
            }
        }

        // 3. Write Parquet files per partition
        Configuration conf = new Configuration();
        String basePath = System.getenv().getOrDefault("PARQUET_DIR", "./data/parquet") + "/";

        for (Map.Entry<String, List<GenericRecord>> entry : partitionedData.entrySet()) {
            String partitionPath = basePath + entry.getKey();
            new File(partitionPath).mkdirs(); // Ensure partition directory exists

            // File naming: timestamp.parquet
            Path file = new Path(partitionPath + "/" + System.currentTimeMillis() + ".parquet");

            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
                    .withSchema(SCHEMA)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {

                for (GenericRecord record : entry.getValue()) {
                    writer.write(record);
                }
            } catch (Exception e) {
                System.err.println("Failed to write Parquet file: " + e.getMessage());
            }
        }

        // 4. Clear the batch for the next round
        parquetBatch.clear();
        System.out.println("Parquet flush complete.");
    }

    private static void startHttpServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        // Endpoint for viewing a specific key
        server.createContext("/view-key", exchange -> {
            String query = exchange.getRequestURI().getQuery(); // e.g., key=1
            String key = query.split("=")[1];
            String value = bitcask.get(key);
            String response = value != null ? value : "Not Found";
            exchange.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(response.getBytes()); }
        });

        // Endpoint for viewing all keys
        server.createContext("/view-all", exchange -> {
            StringBuilder csv = new StringBuilder();
            try {
                for (String k : bitcask.getAllKeys()) {
                    csv.append(k).append(",").append(bitcask.get(k)).append("\n");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            byte[] response = csv.toString().getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(response); }
        });

        server.start();
        System.out.println("API Server started on port 8080");
    }
}