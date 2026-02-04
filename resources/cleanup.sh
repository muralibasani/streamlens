#!/bin/bash
set -e

BOOTSTRAP="localhost:9092"

# List of topics (same as in your create script)
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
loans-rejections-topic
loans-disbursements-topic
loans-repayments-topic
mortgage-applications-topic
mortgage-approvals-topic
mortgage-payments-topic
mortgage-defaults-topic
mortgage-closures-topic
accounts-balance-topic
accounts-ledger-topic
accounts-interest-topic
accounts-statements-topic
accounts-fees-topic
fraud-alerts-topic
fraud-signals-topic
fraud-investigations-topic
fraud-decisions-topic
fraud-feedback-topic
compliance-monitoring-topic
compliance-audit-topic
compliance-reports-topic
compliance-violations-topic
compliance-actions-topic
trading-orders-topic
trading-executions-topic
trading-settlements-topic
trading-positions-topic
trading-risk-topic
investments-portfolio-topic
investments-returns-topic
investments-dividends-topic
investments-rebalancing-topic
investments-recommendations-topic
treasury-cashflow-topic
treasury-liquidity-topic
treasury-forex-topic
treasury-hedging-topic
treasury-funding-topic
branch-operations-topic
atm-transactions-topic
atm-status-topic
atm-cash-levels-topic
atm-faults-topic
notifications-email-topic
notifications-sms-topic
notifications-push-topic
notifications-inapp-topic
notifications-preferences-topic
support-tickets-topic
support-updates-topic
support-resolution-topic
support-feedback-topic
support-escalations-topic
)

# -------------------------------
# Step 1: Stop all background producers and consumers
# -------------------------------
echo "🛑 Stopping all background producers and consumers..."
# List jobs and kill all
#!/bin/bash
set -e

echo "🛑 Stopping all kafka-console-producer and kafka-console-consumer processes..."

# Kill all producers
PRODUCERS=$(pgrep -f kafka-console-producer.sh || true)
if [ -n "$PRODUCERS" ]; then
  echo "Found producer processes: $PRODUCERS"
  echo "$PRODUCERS" | xargs kill -9
  echo "✅ Producers killed."
else
  echo "No producer processes found."
fi

# Kill all consumers
CONSUMERS=$(pgrep -f kafka-console-consumer.sh || true)
if [ -n "$CONSUMERS" ]; then
  echo "Found consumer processes: $CONSUMERS"
  echo "$CONSUMERS" | xargs kill -9
  echo "✅ Consumers killed."
else
  echo "No consumer processes found."
fi

echo "🎉 All Kafka producers and consumers stopped."

# Optional: wait a few seconds
sleep 2
echo "✅ All background producers/consumers stopped."

# -------------------------------
# Step 2: Delete all topics
# -------------------------------
echo "🗑️ Deleting all topics..."
for topic in "${TOPICS[@]}"; do
  echo "Deleting topic: $topic"
  ./bin/kafka-topics.sh \
    --bootstrap-server $BOOTSTRAP \
    --delete \
    --topic "$topic" || echo "⚠️ Could not delete topic $topic (may not exist)"
done
echo "✅ All topics deleted."

# Optional: remove logs
if [ -d logs ]; then
  echo "🧹 Removing logs folder..."
  rm -rf logs
  echo "✅ Logs removed."
fi

echo "🎉 Kafka cleanup completed."
