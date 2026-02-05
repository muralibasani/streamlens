#!/bin/bash
set -e

BOOTSTRAP="localhost:9092"

# Reduced list of topics (matches create_data.sh)
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

echo "🧹 Kafka Cleanup Tool"
echo "===================="
echo ""

# -------------------------------
# Step 1: Stop all background producers and consumers
# -------------------------------
echo "🛑 Stopping all kafka-console-producer and kafka-console-consumer processes..."

# Kill all producers
PRODUCERS=$(pgrep -f kafka-console-producer || true)
if [ -n "$PRODUCERS" ]; then
  echo "   Found producer processes: $PRODUCERS"
  echo "$PRODUCERS" | xargs kill -9 2>/dev/null || true
  echo "   ✅ Producers killed."
else
  echo "   ℹ️  No producer processes found."
fi

# Kill all consumers
CONSUMERS=$(pgrep -f kafka-console-consumer || true)
if [ -n "$CONSUMERS" ]; then
  echo "   Found consumer processes: $CONSUMERS"
  echo "$CONSUMERS" | xargs kill -9 2>/dev/null || true
  echo "   ✅ Consumers killed."
else
  echo "   ℹ️  No consumer processes found."
fi

sleep 1
echo "✅ All background processes stopped."
echo ""

# -------------------------------
# Step 2: Delete topics (optional)
# -------------------------------
echo "🗑️  Delete topics? (y/N): "
read -r DELETE_TOPICS

if [[ "$DELETE_TOPICS" =~ ^[Yy]$ ]]; then
  echo "Deleting ${#TOPICS[@]} topics..."
  for topic in "${TOPICS[@]}"; do
    echo "   Deleting: $topic"
    kafka-topics.sh \
      --bootstrap-server "$BOOTSTRAP" \
      --delete \
      --topic "$topic" 2>/dev/null || echo "   ⚠️  Could not delete $topic (may not exist)"
  done
  echo "✅ Topics deleted."
else
  echo "⏭️  Skipping topic deletion."
fi

echo ""

# -------------------------------
# Step 3: Clean up logs
# -------------------------------
if [ -d logs ]; then
  echo "🧹 Removing logs folder..."
  rm -rf logs
  echo "✅ Logs removed."
fi

echo ""
echo "🎉 Cleanup completed!"
echo ""
echo "To restart with fresh data:"
echo "   ./create_data.sh"
