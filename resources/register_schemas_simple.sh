#!/bin/bash
# Simple schema registration script
# If you get "forwarding to master" errors, restart Schema Registry first

SCHEMA_REGISTRY="http://localhost:8081"

echo "🚀 Registering a few sample schemas..."
echo ""

echo "1. customer-accounts-topic"
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"CustomerAccount\", \"fields\":[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"balance\", \"type\": \"int\"}]}"}' \
  ${SCHEMA_REGISTRY}/subjects/customer-accounts-topic-value/versions
echo ""
echo ""

echo "2. transactions-topic"
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"Transaction\", \"fields\":[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"amount\", \"type\": \"int\"}]}"}' \
  ${SCHEMA_REGISTRY}/subjects/transactions-topic-value/versions
echo ""
echo ""

echo "3. payments-topic"
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"Payment\", \"fields\":[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"amount\", \"type\": \"int\"}]}"}' \
  ${SCHEMA_REGISTRY}/subjects/payments-topic-value/versions
echo ""
echo ""

echo "4. cards-transactions-topic"
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"CardTransaction\", \"fields\":[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"amount\", \"type\": \"int\"}]}"}' \
  ${SCHEMA_REGISTRY}/subjects/cards-transactions-topic-value/versions
echo ""
echo ""

echo "5. loans-applications-topic"
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"LoanApplication\", \"fields\":[{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"amount\", \"type\": \"int\"}]}"}' \
  ${SCHEMA_REGISTRY}/subjects/loans-applications-topic-value/versions
echo ""
echo ""

echo "✅ Done! Check http://localhost:8081/subjects to see registered schemas"
