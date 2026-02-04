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

- **Source**: **None from the broker.** Kafka does not expose “who produces to which topic.” Producers are just clients; the broker does not register them.
- **Options**:
  1. **App registration (no code changes)**  
     Register apps in this UI (or via API): app name + role “producer” + list of topics.  
     Use **Register app** on the cluster / topology, or:
     - `POST /api/clusters/{id}/registrations`  
       Body: `{ "appName": "my-producer", "role": "producer", "topics": ["my-topic"] }`
     Then click **Sync** so the topology is rebuilt with these registrations.
  2. **Interceptors / agent (optional)**  
     You could add a producer interceptor or a small agent that reports “app X produces to topic Y” to a service; this app would then need to ingest that. Not required if you use app registration above.

## Summary

| What        | How it appears                         | What you need                                  |
|------------|----------------------------------------|-------------------------------------------------|
| Topics     | From broker                            | Nothing                                        |
| Consumers  | From broker (consumer groups)          | Consumer with `group.id`, running; then Sync   |
| Producers  | Not from broker                        | Register app (UI or API); then Sync            |

No interceptors are required. Use **Register app** (or the registrations API) for producers; use a normal consumer group for consumers and refresh topology while the consumer is running.
