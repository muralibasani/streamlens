# How topology data is shown

## Topics

- **Source**: Kafka broker (`AdminClient.list_topics()`).
- Topics that exist on the cluster appear automatically. No configuration needed.

## Consumers (who consumes from which topic)

- **Source**: Kafka **consumer groups** (`list_consumer_groups` + `describe_consumer_groups`).
- The broker only knows about **consumer groups** that have (or had) members. Each group has a `group.id` and is assigned topic-partitions.
- **What you need**:
  1. Your consumer must use a **group.id** (standard Kafka consumer config).
  2. The consumer process must be **running** (or have run recently) so the group has members and assignments.
  3. Click **Sync** on the topology page (or wait for the background refresh) so we re-query the broker.
- If you use a consumer **without** a group (e.g. assign partitions manually with no `group.id`), the broker does not track it, so it will not appear. Use a consumer group to see it.

## Producers (who produces to which topic)

- **Source**: Kafka does not expose "who produces to which topic" from the broker. Producers are discovered via:
  1. **JMX** (recommended) — Enable JMX on brokers and configure JMX host/port in the cluster. Topics with active message flow show as producer nodes. See README JMX section.
  2. **ACLs** — If Kafka ACLs are enabled, principals with WRITE permission on topics appear as potential producers.
- Click **Sync** to refresh the topology after configuring JMX or ACLs.

## Summary

| What        | How it appears                         | What you need                                  |
|------------|----------------------------------------|-------------------------------------------------|
| Topics     | From broker                            | Nothing                                        |
| Consumers  | From broker (consumer groups)          | Consumer with `group.id`, running; then Sync   |
| Producers  | JMX or ACLs                            | JMX enabled on brokers, or ACLs; then Sync     |
| Connectors | Kafka Connect REST API                 | Connect URL in cluster; then Sync               |

Use a normal consumer group for consumers and refresh topology while the consumer is running. Enable JMX for producer visibility.
