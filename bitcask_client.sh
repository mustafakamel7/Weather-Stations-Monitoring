#!/bin/bash

CENTRAL_STATION_URL="http://localhost:8080"

if [ "$1" == "--view-all" ]; then
    TIMESTAMP=$(date +%s)
    FILENAME="${TIMESTAMP}.csv"
    echo "key,value" > "$FILENAME"
    curl -s "$CENTRAL_STATION_URL/view-all" >> "$FILENAME"
    echo "Saved to $FILENAME"

elif [ "$1" == "--view" ] && [[ "$2" == --key=* ]]; then
    KEY="${2#*=}"
    curl -s "$CENTRAL_STATION_URL/view-key?key=${KEY}"
    echo ""

elif [ "$1" == "--perf" ] && [[ "$2" == --clients=* ]]; then
    THREADS="${2#*=}"
    echo "Starting $THREADS threads..."
    TIMESTAMP=$(date +%s)
    for i in $(seq 1 $THREADS); do
        (
            FILENAME="${TIMESTAMP}_thread_${i}.csv"
            echo "key,value" > "$FILENAME"
            curl -s "$CENTRAL_STATION_URL/view-all" >> "$FILENAME"
        ) &
    done
    wait
    echo "All $THREADS threads completed."

else
    echo "Usage:"
    echo "  ./bitcask_client.sh --view-all"
    echo "  ./bitcask_client.sh --view --key=STATION_ID"
    echo "  ./bitcask_client.sh --perf --clients=100"
fi