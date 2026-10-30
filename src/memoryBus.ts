import type { Logger } from './logger.ts'
import {
  DEFAULT_RETRY,
  processWithRetry,
  type BusMessage,
  type DeadLetter,
  type EventBus,
  type RetryPolicy,
  type SubscribeOptions,
} from './bus.ts'

export interface MemoryEventBus extends EventBus {
  readonly published: BusMessage[]
  readonly deadLetters: DeadLetter[]
  /** Resolves once every published message has been handled by every group. */
  drain(): Promise<void>
}

/**
 * In-process bus with Kafka-like semantics for tests: each consumer group
 * receives every message on its topics, in publish order, with the same
 * retry/dead-letter behaviour as the Kafka bus.
 */
export function createMemoryEventBus(logger: Logger, retry: RetryPolicy = { ...DEFAULT_RETRY, baseDelayMs: 1 }): MemoryEventBus {
  type Group = { options: SubscribeOptions; tail: Promise<void>; offset: number }
  const groups = new Map<string, Group>()
  const published: BusMessage[] = []
  const deadLetters: DeadLetter[] = []


  const deliverTo = (group: Group, message: BusMessage) => {
    if (!group.options.topics.includes(message.topic)) return
    const offset = String(group.offset++)
    group.tail = group.tail.then(async () => {
      const raw = JSON.stringify(message.envelope)
      const context = { topic: message.topic, partition: 0, offset }
      const outcome = await processWithRetry(raw, context, group.options.groupId, group.options.handler, retry, logger)
      if (outcome.status === 'dead-lettered') deadLetters.push(outcome.deadLetter)
    })
  }

  return {
    published,
    deadLetters,
    async publish(messages) {
      for (const message of messages) {
        // Round-trip through JSON like a real broker would.
        const copy = { ...message, envelope: JSON.parse(JSON.stringify(message.envelope)) }
        published.push(copy)
        for (const group of groups.values()) deliverTo(group, copy)
      }
    },
    async subscribe(options) {
      if (groups.has(options.groupId)) throw new Error(`group ${options.groupId} already subscribed`)
      const group: Group = { options, tail: Promise.resolve(), offset: 0 }
      groups.set(options.groupId, group)
      // Like fallbackMode "earliest": a new group sees earlier messages too (only it: the others have had them).
      for (const message of published) deliverTo(group, message)
      return {
        async close() {
          groups.delete(options.groupId)
        },
      }
    },
    async drain() {
      let before: Promise<void>[]
      do {
        before = [...groups.values()].map((group) => group.tail)
        await Promise.all(before)
      } while ([...groups.values()].some((group, index) => group.tail !== before[index]))
    },
    async healthCheck() {},
    async close() {
      groups.clear()
    },
  }
}
