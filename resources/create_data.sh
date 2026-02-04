#!/bin/bash
set -e

BOOTSTRAP="localhost:9092"
SCHEMA_REGISTRY="http://localhost:8081"

CREATE_TOPICS=true       # set to false to skip topic creation
START_PRODUCERS=true     # set to false to skip starting producers
START_CONSUMERS=true     # set to false to skip starting consumers
REGISTER_SCHEMAS=true     # set to false to skip registering schemas


# Create logs directory
mkdir -p logs

# List of 100 banking/finance topics
TOPICS=(
customer-accounts-topic
customer-profile-topic
customer-kyc-topic
customer-risk-score-topic
customer-notifications-topic
transactions-topic
transaction-audit-topic
transaction-status-topic
transaction-history-topic
transaction-reversals-topic
payments-topic
payment-requests-topic
payment-confirmations-topic
payment-failures-topic
payment-settlements-topic
cards-authorization-topic
cards-transactions-topic
cards-fraud-check-topic
cards-limits-topic
cards-blocks-topic
loans-applications-topic
loans-approvals-topic
# Add more if needed to reach 100
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
      echo "Starting producer for topic: $topic"

      (
        while true; do
          ID=$RANDOM
          AMOUNT=$((RANDOM % 10000))
          TYPE=("payment" "deposit" "withdrawal" "transfer" "important")
          CHOICE=${TYPE[$RANDOM % ${#TYPE[@]}]}
          MSG="{\"id\":$ID,\"amount\":$AMOUNT,\"type\":\"$CHOICE\"}"
          echo "$MSG"
          sleep 0.2
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

# -------------------------------
# Step 4: Register producers with StreamLens API
# -------------------------------
STREAMLENS_API="http://localhost:5000/api"
CLUSTER_ID=1  # Change this to match your cluster ID

echo "🚀 Registering producers with StreamLens..."
for i in "${!TOPICS[@]}"; do
  if (( i % 2 == 0 )); then
    topic="${TOPICS[i]}"
    APP_NAME="producer-${topic}"
    echo "Registering producer: $APP_NAME for topic: $topic"
    
    curl -s -X POST "$STREAMLENS_API/clusters/$CLUSTER_ID/registrations" \
      -H "Content-Type: application/json" \
      -d "{\"appName\": \"$APP_NAME\", \"role\": \"producer\", \"topics\": [\"$topic\"]}" \
      > /dev/null 2>&1
  fi
done
echo "✅ Producers registered with StreamLens."

echo "🛠️ Done. Use 'jobs' to see running processes or 'kill %N' to stop individual producers/consumers."

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
