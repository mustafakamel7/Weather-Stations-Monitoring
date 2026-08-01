# Weather Project

A Java-based weather data pipeline for the Designing Data-Intensive Applications lab. The project simulates multiple weather stations, streams readings through Kafka, stores the latest station state in a custom Bitcask-style key-value store, archives history to Parquet, and detects rain alerts with Kafka Streams.

## What Is Implemented

- 10 simulated weather stations that publish readings to Kafka once per second
- An Open-Meteo adapter that polls real weather data and publishes it as virtual station `11`
- A central station consumer that stores the latest reading per station in Bitcask and batches all readings into Parquet files
- A Kafka Streams rain detector that sends high-humidity readings to the `rain-alerts` topic
- A dead-letter topic for producer or adapter failures
- A small HTTP API for reading Bitcask state
- Docker Compose and Kubernetes manifests for local deployment
- A Python script for loading Parquet history into Elasticsearch
- Kibana support for dashboarding indexed historical data

## Architecture

![Architecture Overview](https://github.com/user-attachments/assets/65eeadd0-75b7-4144-9a72-0ef4d9090a93)

```text
weather-station containers
        |
        v
Kafka topic: weather  <--- openmeteo-adapter
        |
        +--> central-station --> Bitcask latest-state store
        |                    --> Parquet historical files
        |
        +--> rain-detector --> Kafka topic: rain-alerts

Parquet files --> parquet_to_es.py --> Elasticsearch --> Kibana
```

## Services

| Service | Path | Purpose |
|---|---|---|
| `weather-station` | `weather-station/` | Produces simulated station readings to Kafka topic `weather`. |
| `central-station` | `central-station/` | Consumes `weather`, writes latest values to Bitcask, archives all records to Parquet, and exposes HTTP endpoints on port `8080`. |
| `rain-detector` | `rain-detector/` | Kafka Streams app that filters readings where humidity is greater than `70` and publishes alerts to `rain-alerts`. |
| `openmeteo-adapter` | `openmeteo-adapter/` | Polls the Open-Meteo API for Cairo by default and publishes the result to `weather` as station `11`. |
| `parquet_to_es.py` | root | Reads generated Parquet files and bulk-indexes them into Elasticsearch index `weather_history`. |

## Message Format

The producers currently send a flat JSON object:

```json
{
  "station_id": 1,
  "s_no": 1,
  "battery_status": "medium",
  "status_timestamp": 1716470000,
  "weather": {
    "humidity": 42,
    "temperature": 83,
    "wind_speed": 18
  }
}
```

The Open-Meteo adapter uses the same shape and adds:

```json
{
  "source": "open-meteo"
}
```

## Data Generation

Each simulated weather station:

- Uses `STATION_ID` from the environment
- Connects to Kafka through `KAFKA_BOOTSTRAP`
- Publishes to topic `weather`
- Sends one message per second
- Simulates a 10% message drop rate
- Generates battery status with this distribution:
  - `low`: 30%
  - `medium`: 40%
  - `high`: 30%

The generated weather values are:

- `humidity`: random integer from `0` to `100`
- `temperature`: random integer from `60` to `120`
- `wind_speed`: random integer from `0` to `100`

## Storage

### Bitcask Latest-State Store

`central-station` writes the newest message for each station key into a custom Bitcask implementation.

Implemented behavior:

- Append-only `.data` files
- In-memory `KeyDir` map for direct key lookup
- Compaction when the active data file grows beyond 1 MB
- `.hint` files written during compaction
- Hint-file loading on startup

The Bitcask directory defaults to:

```text
./data/bitcask
```

Override it with:

```text
BITCASK_DIR=/some/path
```

### Parquet History

`central-station` also batches records and writes them to Snappy-compressed Parquet files.

Implemented behavior:

- Batch size: `1000` records
- Output directory default: `./data/parquet`
- Partition format:

```text
time=YYYY-MM-DD HH:00/station_id=N/<timestamp>.parquet
```

Override the Parquet root with:

```text
PARQUET_DIR=/some/path
```

## HTTP API

The central station exposes a small HTTP API on port `8080`.

| Endpoint | Description |
|---|---|
| `GET /view-key?key=STATION_ID` | Returns the latest Bitcask value for one station. |
| `GET /view-all` | Returns all Bitcask keys and values as CSV rows. |

Helper script:

```bash
./bitcask_client.sh --view --key=1
./bitcask_client.sh --view-all
./bitcask_client.sh --perf --clients=100
```

## Running With Docker Compose

Build the Java jars:

```bash
cd weather-station && mvn package && cd ..
cd central-station && mvn package && cd ..
cd rain-detector && mvn package && cd ..
cd openmeteo-adapter && mvn package && cd ..
```

Build the local Docker images expected by `docker-compose.yml`:

```bash
docker build -t weather-station:1.0 ./weather-station
docker build -t central-station:1.0 ./central-station
docker build -t rain-detector:1.0 ./rain-detector
docker build -t openmeteo-adapter:1.0 ./openmeteo-adapter
```

Start the stack:

```bash
docker compose up
```

Compose starts:

- Kafka `3.7.0` in KRaft mode
- 10 weather station containers
- Central station on `localhost:8080`
- Rain detector
- Open-Meteo adapter
- Elasticsearch on `localhost:9200`
- Kibana on `localhost:5601`

## Loading History Into Elasticsearch

After Parquet files have been written, run:

```bash
python parquet_to_es.py
```

The script reads Parquet files from:

```text
~/weather-project/central-station/data/parquet
```

and writes documents to Elasticsearch index:

```text
weather_history
```

Each Elasticsearch document ID is built from:

```text
station_id_s_no
```

## Kubernetes Deployment

The `k8s/` directory contains two manifest files:

| File | Contents |
|---|---|
| `k8s/infra.yaml` | Kafka, Elasticsearch, and Kibana deployments/services. |
| `k8s/apps.yaml` | PVC, central station, rain detector, and 10 station deployments. |

Build and load the local images into your local Kubernetes runtime, then apply:

```bash
kubectl apply -f k8s/infra.yaml
kubectl apply -f k8s/apps.yaml
```

The Kubernetes manifests use `imagePullPolicy: Never`, so the images must already exist inside the cluster runtime.

Exposed NodePorts:

| Service | NodePort |
|---|---|
| Kibana | `30000` |
| Central station | `30080` |

The central station pod also starts a 60-second Java Flight Recorder capture at:

```text
/app/data/profile.jfr
```

## Environment Variables

| Variable | Used By | Default | Description |
|---|---|---|---|
| `STATION_ID` | `weather-station` | `1` | Station identifier used as Kafka key. |
| `KAFKA_BOOTSTRAP` | all Java services | varies by service | Kafka bootstrap server. |
| `BITCASK_DIR` | `central-station` | `./data/bitcask` | Directory for Bitcask data and hint files. |
| `PARQUET_DIR` | `central-station` | `./data/parquet` | Directory for Parquet archives. |
| `LATITUDE` | `openmeteo-adapter` | `30.0444` | Open-Meteo latitude. |
| `LONGITUDE` | `openmeteo-adapter` | `31.2357` | Open-Meteo longitude. |

## Enterprise Integration Patterns

| Pattern | Implementation |
|---|---|
| Channel Adapter | `openmeteo-adapter` converts Open-Meteo API responses into the internal weather message format. |
| Polling Consumer | `openmeteo-adapter` polls Open-Meteo once per second. |
| Event-Driven Consumer | `rain-detector` reacts to messages from Kafka topic `weather`. |
| Message Filter | `rain-detector` filters for humidity greater than `70`. |
| Dead Letter Channel | Failed producer/adapter messages are sent to Kafka topic `dead-letter`. |
| Idempotent Receiver | `central-station` skips readings whose `s_no` is not newer than the last processed sequence for that station. |

## Repository Layout

```text
.
|-- central-station/
|-- weather-station/
|-- rain-detector/
|-- openmeteo-adapter/
|-- k8s/
|   |-- apps.yaml
|   `-- infra.yaml
|-- docker-compose.yml
|-- bitcask_client.sh
|-- parquet_to_es.py
`-- README.md
```

## Contributors

- [Moustafa Kamel](https://github.com/mustafakamel7)
- [Ahmed Emara](https://github.com/Emara25)
- [Mohamed Rezq](https://github.com/MRezq788)
- [Ahmed Ali](https://github.com/a7mdli)
