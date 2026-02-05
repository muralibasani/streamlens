#!/bin/bash
set -e

BOOTSTRAP="localhost:9092"
SCHEMA_REGISTRY="http://localhost:8081"

# Toggle features on/off
CREATE_TOPICS=false       # set to false to skip topic creation
START_PRODUCERS=false     # set to false to skip starting producers
START_CONSUMERS=false     # set to false to skip starting consumers
REGISTER_SCHEMAS=true   # set to false to skip registering schemas

# Performance tuning - adjust based on your system resources
# ⚠️  Default settings are optimized for local development
MESSAGE_INTERVAL=2       # seconds between messages (2 = low load, 0.5 = medium, 0.1 = high)
MAX_MESSAGES=100         # max messages per producer (100 = demo, 0 = unlimited)
                         # With 4 producers: 100 msgs * 4 = 400 total messages

# Resource usage examples:
# - Light load (for older Macs):  MESSAGE_INTERVAL=5,  MAX_MESSAGES=50
# - Default (recommended):        MESSAGE_INTERVAL=2,  MAX_MESSAGES=100
# - Heavy load (stress test):     MESSAGE_INTERVAL=0.5, MAX_MESSAGES=0 (unlimited)

# Create logs directory
mkdir -p logs

# Reduced list of topics for demo (8 topics -> 4 producers/consumers)
# Increase this list if you need more data
TOPICS=(
customer-accounts-topic
transactions-topic
payments-topic
cards-authorization-topic
loans-applications-topic
notifications-topic
fraud-detection-topic
audit-log-topic
)

# -------------------------------
# Step 1: Create topics
# -------------------------------
if [ "$CREATE_TOPICS" = true ]; then
  echo "🚀 Creating topics..."
  for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    kafka-topics.sh \
      --bootstrap-server $BOOTSTRAP \
      --create \
      --if-not-exists \
      --topic "$topic" \
      --partitions 3 \
      --replication-factor 1
  done
  echo "✅ All topics created."
else
  echo "⚠️ Skipping topic creation."
fi

# -------------------------------
# Step 2: Start producers for alternate topics
# -------------------------------
if [ "$START_PRODUCERS" = true ]; then
  echo "🚀 Starting producers for alternate topics..."
  for i in "${!TOPICS[@]}"; do
    if (( i % 2 == 0 )); then
      topic="${TOPICS[i]}"
      echo "Starting producer for topic: $topic (interval: ${MESSAGE_INTERVAL}s, max: ${MAX_MESSAGES:-unlimited})"

      (
        count=0
        while true; do
          # Check message limit (0 means unlimited)
          if [ "$MAX_MESSAGES" -gt 0 ] && [ "$count" -ge "$MAX_MESSAGES" ]; then
            echo "Reached max messages ($MAX_MESSAGES), stopping producer for $topic"
            break
          fi
          
          ID=$RANDOM
          AMOUNT=$((RANDOM % 10000))
          TYPE=("payment" "deposit" "withdrawal" "transfer" "important")
          CHOICE=${TYPE[$RANDOM % ${#TYPE[@]}]}
          MSG="{\"id\":$ID,\"amount\":$AMOUNT,\"type\":\"$CHOICE\"}"
          echo "$MSG"
          
          count=$((count + 1))
          sleep "$MESSAGE_INTERVAL"
        done
      ) | kafka-console-producer.sh \
           --bootstrap-server $BOOTSTRAP \
           --topic "$topic" > logs/${topic}-producer.log 2>&1 &
    fi
  done
  echo "✅ Producers started in background."
else
  echo "⚠️ Skipping producers."
fi

# -------------------------------
# Step 3: Start consumers for same alternate topics
# -------------------------------
if [ "$START_CONSUMERS" = true ]; then
  echo "🚀 Starting consumers for alternate topics..."
  for i in "${!TOPICS[@]}"; do
    if (( i % 2 == 0 )); then
      topic="${TOPICS[i]}"
      GROUP_NAME="consumer-${topic}-group"
      echo "Starting consumer for topic: $topic with group: $GROUP_NAME"

      kafka-console-consumer.sh \
        --bootstrap-server $BOOTSTRAP \
        --topic "$topic" \
        --group "$GROUP_NAME" \
        --from-beginning \
        > logs/${topic}-consumer.log 2>&1 &
    fi
  done
  echo "✅ Consumers started in background."
else
  echo "⚠️ Skipping consumers."
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "📊 Statistics:"
echo "   - Topics created: ${#TOPICS[@]}"
echo "   - Producers started: $((${#TOPICS[@]} / 2))"
echo "   - Consumers started: $((${#TOPICS[@]} / 2))"
echo "   - Message rate: 1 msg every ${MESSAGE_INTERVAL}s per producer"
echo "   - Max messages: ${MAX_MESSAGES:-unlimited} per producer"
echo ""
echo "🔧 Management:"
echo "   - View running processes: jobs"
echo "   - Stop all: pkill -f kafka-console-producer && pkill -f kafka-console-consumer"
echo "   - Stop one: kill %N (use 'jobs' to find N)"
echo "   - Or use: ./cleanup.sh"
echo ""
echo "💡 To reduce system load, edit this script and:"
echo "   - Increase MESSAGE_INTERVAL (e.g., 5 seconds)"
echo "   - Reduce MAX_MESSAGES (e.g., 50)"
echo "   - Reduce number of topics in TOPICS array"

if [ "$REGISTER_SCHEMAS" = true ]; then
  echo "🚀 Registering schemas for alternate topics..."

  for i in "${!TOPICS[@]}"; do
    if (( i % 2 == 0 )); then
      topic="${TOPICS[i]}"
      echo "Registering schema for topic: $topic"

      # Use a temporary file to avoid shell escaping issues
      cat > /tmp/schema_payload_$i.json <<EOF
{"schema": "{\"type\": \"record\", \"name\": \"Obj$i\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}, {\"name\": \"index\", \"type\": \"int\", \"default\": $i}]}"}
EOF

      
      # Send the request using the file
      curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data @/tmp/schema_payload_$i.json \
        http://localhost:8081/subjects/${topic}-value/versions

      echo ""
      
      # Cleanup temp file
      rm -f /tmp/schema_payload_$i.json
      
      # Add delay to avoid overwhelming Schema Registry
      sleep 1
    fi
  done

  echo "✅ All schemas registered for alternate topics."
else
  echo "⚠️ Skipping schema registration."
fi
