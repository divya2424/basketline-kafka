import { Admin, Consumer, Producer, ProduceAcks, stringDeserializers, stringSerializers } from '@platformatic/kafka'
import type { Logger } from './logger.ts'
import {
  DEFAULT_RETRY,
  dlqTopic,
  processWithRetry,
  type BusMessage,
  type EventBus,
  type RetryPolicy,
  type SubscribeOptions,
  type Subscription,
} from './bus.ts'

export interface KafkaBusOptions {
  brokers: string[]
  clientId: string
  logger: Logger
  /** Prepended to every topic name; isolates environments and test runs. */
  topicPrefix?: string
  retry?: RetryPolicy
  /** Called when a consumer loop dies; services should exit and be restarted. */
  onFatal?: (error: unknown) => void
}

/** True if the error (or any nested error) is Kafka's TOPIC_ALREADY_EXISTS. */
function isTopicAlreadyExists(error: unknown): boolean {
  const e = error as { apiId?: string; errors?: unknown[] } | null
  return e?.apiId === 'TOPIC_ALREADY_EXISTS' || (Array.isArray(e?.errors) && e.errors.some(isTopicAlreadyExists))
}

/** A topic name, or a name with topic settings (e.g. retention.ms) applied when it is created. */
export type TopicSpec = string | { name: string; configs: Record<string, string> }

/**
 * Creates topics that don't exist yet. Auto-creation is disabled on the
 * broker so partition counts are explicit. 3 partitions per topic; replication
 * 1 locally (set KAFKA_REPLICATION in production). Settings apply only when a
 * topic is created; an existing topic keeps its own.
 */
export async function ensureTopics(
  options: Pick<KafkaBusOptions, 'brokers' | 'clientId' | 'topicPrefix'>,
  topics: TopicSpec[],
  partitions = 3,
  replicas = replicationFromEnv(),
): Promise<void> {
  const admin = new Admin({ clientId: `${options.clientId}-admin`, bootstrapBrokers: options.brokers })
  try {
    const existing = new Set(await admin.listTopics())
    const missing = topics
      .map((topic) => (typeof topic === 'string' ? { name: topic, configs: {} } : topic))
      .map(({ name, configs }) => ({ name: `${options.topicPrefix ?? ''}${name}`, configs }))
      .filter(({ name }) => !existing.has(name))
    // One at a time, so a topic another service just created doesn't fail the rest.
    for (const { name: topic, configs } of missing) {
      try {
        const settings = Object.entries(configs).map(([name, value]) => ({ name, value }))
        await admin.createTopics({ topics: [topic], partitions, replicas, configs: settings })
      } catch (error) {
        if (!isTopicAlreadyExists(error)) throw error
      }
    }
  } finally {
    await admin.close()
  }
}

export async function createKafkaEventBus(options: KafkaBusOptions): Promise<EventBus> {
  const prefix = options.topicPrefix ?? ''
  const retry = options.retry ?? DEFAULT_RETRY
  const { logger } = options
  const producer = new Producer({
    clientId: options.clientId,
    bootstrapBrokers: options.brokers,
    serializers: stringSerializers,
    idempotent: true,
    acks: ProduceAcks.ALL,
  })
  const subscriptions: Subscription[] = []

  const publish = async (messages: BusMessage[]) => {
    if (messages.length === 0) return
    await producer.send({
      messages: messages.map(({ topic, key, envelope }) => ({
        topic: `${prefix}${topic}`,
        key,
        value: JSON.stringify(envelope),
        headers: { eventType: envelope.eventType, eventId: envelope.eventId, producer: envelope.producer },
      })),
    })
  }

  const subscribe = async ({ groupId, topics, handler }: SubscribeOptions): Promise<Subscription> => {
    const dlq = dlqTopic(groupId)
    await ensureTopics(options, [...topics, dlq])
    const consumer = new Consumer({
      groupId: `${prefix}${groupId}`,
      clientId: `${options.clientId}-${groupId}`,
      bootstrapBrokers: options.brokers,
      deserializers: stringDeserializers,
    })
    const stream = await consumer.consume({
      topics: topics.map((topic) => `${prefix}${topic}`),
      autocommit: false,
      mode: 'committed',
      fallbackMode: 'earliest',
      sessionTimeout: 10_000,
      heartbeatInterval: 1_000,
    })

    let closing = false
    // Messages are handled one at a time so per-partition order is kept.
    const loop = (async () => {
      for await (const message of stream) {
        const context = {
          topic: message.topic.slice(prefix.length),
          partition: message.partition,
          offset: message.offset.toString(),
        }
        const outcome = await processWithRetry(String(message.value ?? ''), context, groupId, handler, retry, logger)
        if (outcome.status === 'dead-lettered') {
          await producer.send({
            messages: [
              {
                topic: `${prefix}${dlq}`,
                key: String(message.key ?? ''),
                value: JSON.stringify(outcome.deadLetter),
                headers: { sourceTopic: message.topic, error: outcome.deadLetter.error.slice(0, 500) },
              },
            ],
          })
        }
        await message.commit()
      }
    })()
    loop.catch((error: unknown) => {
      if (closing) return
      logger.fatal({ err: error, groupId }, 'kafka consumer loop failed')
      options.onFatal?.(error)
    })

    const subscription: Subscription = {
      async close() {
        closing = true
        await stream.close().catch(() => {})
        await consumer.close(true).catch(() => {})
      },
    }
    subscriptions.push(subscription)
    logger.info({ groupId, topics }, 'kafka consumer started')
    return subscription
  }

  return {
    publish,
    subscribe,
    async healthCheck() {
      // A metadata request is a real round-trip to the broker.
      await producer.metadata({ topics: [] })
    },
    async close() {
      await Promise.all(subscriptions.map((subscription) => subscription.close()))
      await producer.close()
    },
  }
}

/** KAFKA_REPLICATION, or 1. */
function replicationFromEnv(): number {
  const raw = process.env.KAFKA_REPLICATION
  if (raw === undefined || raw === '') return 1
  const value = Number(raw)
  if (!Number.isInteger(value)) throw new Error('Environment variable KAFKA_REPLICATION must be an integer')
  return value
}
