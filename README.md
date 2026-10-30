# @divya2424/basketline-kafka

The event bus [Basketline](https://github.com/divya2424/basketline)'s services use on Kafka:

- **Publishing:** event envelopes go out in order. `publish` resolves once the broker has acknowledged them all.
- **Consuming:** each consumer group gets every message on its topics. A failing handler is retried with exponential backoff, then the message goes to a dead-letter topic (`dlq.<groupId>`), so one bad message can't block a partition.
- **Topics:** created explicitly with `ensureTopics`, with their partition count and settings such as `retention.ms`.
- **Tests:** an in-memory bus with the same behaviour, so services can be tested without Kafka.

Built on [`@platformatic/kafka`](https://github.com/platformatic/kafka). Node 24 or later; ESM only.

## Install

The package is on GitHub Packages, which needs a token even to install a public package:

1. Create a GitHub personal access token (classic) with `read:packages`.
2. Add an `.npmrc` next to your `package.json`:
   ```
   @divya2424:registry=https://npm.pkg.github.com
   //npm.pkg.github.com/:_authToken=${NODE_AUTH_TOKEN}
   ```
3. Install with the token in the environment:
   ```bash
   export NODE_AUTH_TOKEN=<your token>
   npm install @divya2424/basketline-kafka
   ```

**In Docker,** pass the token as a build secret so it never ends up in an image layer:
```dockerfile
COPY package.json package-lock.json .npmrc ./
RUN --mount=type=secret,id=npm_token NODE_AUTH_TOKEN="$(cat /run/secrets/npm_token)" npm ci --omit=dev
```

## Use

```ts
import { createKafkaEventBus, ensureTopics, type EventEnvelope } from '@divya2424/basketline-kafka'
import { pino } from 'pino'

const logger = pino()
const brokers = ['localhost:9092']

// Topics are created explicitly (3 partitions), with their settings if given.
await ensureTopics({ brokers, clientId: 'orders' }, ['order.v1', { name: 'audit.v1', configs: { 'retention.ms': '604800000' } }])

const bus = await createKafkaEventBus({ brokers, clientId: 'orders', logger, onFatal: () => process.exit(1) })

const envelope: EventEnvelope<{ orderId: string }> = {
  eventId: crypto.randomUUID(),
  eventType: 'OrderPlaced',
  eventVersion: 1,
  occurredAt: new Date().toISOString(),
  producer: 'orders',
  aggregateId: 'o-1',
  aggregateVersion: 1,
  payload: { orderId: 'o-1' },
}
// The key keeps one aggregate's events in order (same partition).
await bus.publish([{ topic: 'order.v1', key: envelope.aggregateId, envelope }])

await bus.subscribe({
  groupId: 'billing',
  topics: ['order.v1'],
  handler: async (event, { topic, partition, offset }) => {
    // Throwing retries the message, then dead-letters it to dlq.billing.
  },
})
```

| Export | What it is |
|---|---|
| `createKafkaEventBus(options)` | The bus on Kafka. Options: `brokers`, `clientId`, `logger`, plus `topicPrefix` (prepended to every topic, to isolate environments or test runs), `retry` and `onFatal` (called if a consumer loop dies; exit and let the process restart) |
| `ensureTopics({ brokers, clientId, topicPrefix? }, topics)` | Creates the topics that don't exist yet: 3 partitions each, replication from `KAFKA_REPLICATION` (default 1). An existing topic keeps its settings |
| `createMemoryEventBus(logger, retry?)` | The in-memory bus for tests. Also has `published`, `deadLetters` and `drain()` (resolves once every group has handled everything) |
| `EventEnvelope`, `isEnvelope` | The message shape, and a check for it. A message that isn't an envelope is dead-lettered at once, without retries |
| `processWithRetry`, `DEFAULT_RETRY`, `dlqTopic` | The retry and dead-letter logic both buses share |
| `Logger` | What the bus logs with: `info`, `warn`, `error` and `fatal`, taking `(details, message)`. A pino logger fits |

**Retries:** `DEFAULT_RETRY` makes 5 attempts, waiting 200 ms and then doubling each time, capped at 5 s. A dead letter records the group, source topic, partition, offset, raw value, error and number of attempts.

## Develop and publish

```bash
pnpm install
pnpm test             # Vitest
pnpm typecheck
pnpm build        # dist/: JavaScript and .d.ts
npm pack             # divya2424-basketline-kafka-<version>.tgz, to try in another project
```

**Publishing:** bump `version` in `package.json`, push, and create a GitHub release `vX.Y.Z`. The `publish` workflow runs the typecheck, tests and build, then publishes to GitHub Packages. `ci` runs the same checks on every push and pull request.

Then, in Basketline's `backend/`: `pnpm --filter @basketline/platform --filter @basketline/contracts add @divya2424/basketline-kafka@<version>`, and rebuild the service images.
