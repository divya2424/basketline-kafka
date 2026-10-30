/** What every message on the bus carries: the event, and who produced which version of what. */
export interface EventEnvelope<P = unknown> {
  eventId: string
  eventType: string
  eventVersion: number
  occurredAt: string
  producer: string
  aggregateId: string
  aggregateVersion: number
  payload: P
}

/** Basic shape check for a message read from Kafka. */
export function isEnvelope(value: unknown): value is EventEnvelope {
  const envelope = value as Partial<EventEnvelope> | null
  return (
    typeof envelope === 'object' &&
    envelope !== null &&
    typeof envelope.eventId === 'string' &&
    typeof envelope.eventType === 'string' &&
    typeof envelope.eventVersion === 'number' &&
    typeof envelope.occurredAt === 'string' &&
    typeof envelope.producer === 'string' &&
    typeof envelope.aggregateId === 'string' &&
    typeof envelope.aggregateVersion === 'number' &&
    typeof envelope.payload === 'object' &&
    envelope.payload !== null
  )
}
