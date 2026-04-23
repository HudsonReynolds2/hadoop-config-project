#!/usr/bin/env bash
# Test 04: Kafka topic create, produce 10 messages, consume 10 messages.

set -euo pipefail
TEST_NAME="04-kafka"
TOPIC="cfgcheck-smoke-$$"
BROKER="localhost:9092"

KBIN=/opt/kafka/bin

echo "[$TEST_NAME] creating topic $TOPIC"
docker exec kafka $KBIN/kafka-topics.sh --bootstrap-server $BROKER \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null

echo "[$TEST_NAME] producing 10 messages"
docker exec -i kafka bash -c \
  "seq 1 10 | $KBIN/kafka-console-producer.sh --bootstrap-server $BROKER --topic $TOPIC >/dev/null 2>&1"

echo "[$TEST_NAME] consuming (timeout 10s)"
OUT=$(docker exec kafka $KBIN/kafka-console-consumer.sh \
  --bootstrap-server $BROKER --topic "$TOPIC" --from-beginning \
  --max-messages 10 --timeout-ms 10000 2>/dev/null || true)

COUNT=$(echo "$OUT" | grep -c '^[0-9]' || true)

# Cleanup
docker exec kafka $KBIN/kafka-topics.sh --bootstrap-server $BROKER \
  --delete --topic "$TOPIC" >/dev/null 2>&1 || true

if [ "$COUNT" -eq 10 ]; then
  echo "[$TEST_NAME] PASS: produced 10, consumed 10"
  exit 0
else
  echo "[$TEST_NAME] FAIL: consumed $COUNT of 10"
  exit 1
fi
