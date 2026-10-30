import { setTimeout as sleep } from 'node:timers/promises'
import { isEnvelope, type EventEnvelope } from './envelope.ts'
import type { Logger } from './logger.ts'

export interface BusMessage {
  topic: string
  key: string
  envelope: EventEnvelope
}

export interface DeliveryContext {
  topic: string
  partition: number
  offset: string
}

export type EventHandler = (envelope: EventEnvelope, context: DeliveryContext) => Promise<void>

export interface SubscribeOptions {
  /** Kafka consumer group; one per consuming service. */
  groupId: string
  topics: string[]
  handler: EventHandler
}

export interface Subscription {
  close(): Promise<void>
}

export interface EventBus {
  /** Publishes in order; resolves once the broker has acknowledged all messages. */
  publish(messages: BusMessage[]): Promise<void>
  subscribe(options: SubscribeOptions): Promise<Subscription>
  /** Throws if the broker can't be reached (used by readiness probes). */
  healthCheck(): Promise<void>
  close(): Promise<void>
}

export interface RetryPolicy {
  maxAttempts: number
  baseDelayMs: number
  maxDelayMs: number
}

export const DEFAULT_RETRY: RetryPolicy = { maxAttempts: 5, baseDelayMs: 200, maxDelayMs: 5_000 }

export function dlqTopic(groupId: string): string {
  return `dlq.${groupId}`
}

export interface DeadLetter {
  groupId: string
  sourceTopic: string
  partition: number
  offset: string
  rawValue: string
  error: string
  attempts: number
  failedAt: string
}

export type ProcessOutcome = { status: 'processed' } | { status: 'dead-lettered'; deadLetter: DeadLetter }

/**
 * Parses and handles one raw message with bounded exponential backoff.
 * Malformed messages skip retries (they can never succeed). Never throws:
 * the caller dead-letters failures and commits, so one poison message can't
 * block the partition forever.
 */
export async function processWithRetry(
  rawValue: string,
  context: DeliveryContext,
  groupId: string,
  handler: EventHandler,
  retry: RetryPolicy,
  logger: Logger,
): Promise<ProcessOutcome> {
  const deadLetter = (error: unknown, attempts: number): ProcessOutcome => ({
    status: 'dead-lettered',
    deadLetter: {
      groupId,
      sourceTopic: context.topic,
      partition: context.partition,
      offset: context.offset,
      rawValue,
      error: error instanceof Error ? error.message : String(error),
      attempts,
      failedAt: new Date().toISOString(),
    },
  })

  let envelope: EventEnvelope
  try {
    const parsed: unknown = JSON.parse(rawValue)
    if (!isEnvelope(parsed)) throw new Error('message is not an event envelope')
    envelope = parsed
  } catch (error) {
    logger.error({ err: error, ...context }, 'malformed event; dead-lettering')
    return deadLetter(error, 0)
  }

  for (let attempt = 1; attempt <= retry.maxAttempts; attempt += 1) {
    try {
      await handler(envelope, context)
      return { status: 'processed' }
    } catch (error) {
      const log = { err: error, eventId: envelope.eventId, eventType: envelope.eventType, attempt, ...context }
      if (attempt === retry.maxAttempts) {
        logger.error(log, 'event handling failed after retries; dead-lettering')
        return deadLetter(error, attempt)
      }
      logger.warn(log, 'event handling failed; retrying')
      await sleep(Math.min(retry.maxDelayMs, retry.baseDelayMs * 2 ** (attempt - 1)))
    }
  }
  throw new Error('unreachable')
}
