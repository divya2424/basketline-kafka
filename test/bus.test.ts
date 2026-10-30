import { describe, expect, it, vi } from 'vitest'
import { createMemoryEventBus, isEnvelope, processWithRetry, type EventEnvelope, type Logger } from '../src/index.ts'

const logger: Logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), fatal: vi.fn() }
const retry = { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 1 }
const context = { topic: 'orders.v1', partition: 0, offset: '7' }

function envelope(eventId: string, payload: object = { n: 1 }): EventEnvelope {
  return { eventId, eventType: 'OrderPlaced', eventVersion: 1, occurredAt: '2026-10-30T10:00:00.000Z', producer: 'test', aggregateId: 'o-1', aggregateVersion: 1, payload }
}

describe('processWithRetry', () => {
  it('retries a failing handler, then dead-letters it with the reason and the attempts', async () => {
    const handler = vi.fn(async () => {
      throw new Error('database down')
    })
    const outcome = await processWithRetry(JSON.stringify(envelope('e-1')), context, 'billing', handler, retry, logger)
    expect(handler).toHaveBeenCalledTimes(3)
    expect(outcome).toMatchObject({ status: 'dead-lettered', deadLetter: { groupId: 'billing', sourceTopic: 'orders.v1', offset: '7', error: 'database down', attempts: 3 } })
  })

  it('succeeds once the handler does', async () => {
    const handler = vi.fn().mockRejectedValueOnce(new Error('blip')).mockResolvedValueOnce(undefined)
    expect(await processWithRetry(JSON.stringify(envelope('e-2')), context, 'billing', handler, retry, logger)).toEqual({ status: 'processed' })
    expect(handler).toHaveBeenCalledTimes(2)
  })

  it('dead-letters malformed messages at once, without calling the handler', async () => {
    const handler = vi.fn()
    for (const raw of ['not json', JSON.stringify({ eventId: 'x' })]) {
      expect(await processWithRetry(raw, context, 'billing', handler, retry, logger)).toMatchObject({ status: 'dead-lettered', deadLetter: { attempts: 0, rawValue: raw } })
    }
    expect(handler).not.toHaveBeenCalled()
  })
})

describe('the in-memory bus', () => {
  it('delivers each topic to every subscribed group in publish order, including earlier messages', async () => {
    const bus = createMemoryEventBus(logger, retry)
    await bus.publish([{ topic: 'orders.v1', key: 'o-1', envelope: envelope('e-1') }])
    const billing: string[] = []
    const shipping: string[] = []
    await bus.subscribe({ groupId: 'billing', topics: ['orders.v1'], handler: async (e) => void billing.push(e.eventId) })
    await bus.subscribe({ groupId: 'shipping', topics: ['orders.v1'], handler: async (e) => void shipping.push(e.eventId) })
    await bus.publish([
      { topic: 'orders.v1', key: 'o-1', envelope: envelope('e-2') },
      { topic: 'other.v1', key: 'o-1', envelope: envelope('e-3') },
    ])
    await bus.drain()
    expect(billing).toEqual(['e-1', 'e-2'])
    expect(shipping).toEqual(['e-1', 'e-2'])
    expect(bus.published.map((m) => m.envelope.eventId)).toEqual(['e-1', 'e-2', 'e-3'])
  })

  it('dead-letters what a group keeps failing on, and moves on', async () => {
    const bus = createMemoryEventBus(logger, retry)
    const handled: string[] = []
    await bus.subscribe({
      groupId: 'billing',
      topics: ['orders.v1'],
      handler: async (e) => {
        if (e.eventId === 'poison') throw new Error('cannot bill')
        handled.push(e.eventId)
      },
    })
    await bus.publish([
      { topic: 'orders.v1', key: 'o-1', envelope: envelope('poison') },
      { topic: 'orders.v1', key: 'o-1', envelope: envelope('e-4') },
    ])
    await bus.drain()
    expect(handled).toEqual(['e-4'])
    expect(bus.deadLetters).toEqual([expect.objectContaining({ groupId: 'billing', error: 'cannot bill', attempts: 3 })])
  })

  it('checks the envelope shape', () => {
    expect(isEnvelope(envelope('e-5'))).toBe(true)
    expect(isEnvelope({ ...envelope('e-6'), payload: null })).toBe(false)
  })
})
