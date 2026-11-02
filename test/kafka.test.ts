import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Logger } from '../src/index.ts'

/** A stand-in for @platformatic/kafka: consumers whose streams wait until closed, and how many were started. */
const kafka = vi.hoisted(() => ({ consumers: [] as any[], failConsume: 0 }))

vi.mock('@platformatic/kafka', async () => {
  const { EventEmitter } = await import('node:events')
  class Stream extends EventEmitter {
    closed = false
    #end?: () => void
    async close() {
      this.closed = true
      this.#end?.()
    }
    [Symbol.asyncIterator]() {
      return { next: () => new Promise<{ done: true; value: undefined }>((resolve) => (this.#end = () => resolve({ done: true, value: undefined }))) }
    }
  }
  class Consumer extends EventEmitter {
    closed = false
    stream = new Stream()
    constructor() {
      super()
      kafka.consumers.push(this)
    }
    async consume() {
      if (kafka.failConsume > 0) {
        kafka.failConsume -= 1
        throw new Error('broker unavailable')
      }
      return this.stream
    }
    async close() {
      this.closed = true
    }
  }
  class Admin {
    async listTopics() {
      return []
    }
    async createTopics() {}
    async close() {}
  }
  class Producer {
    async send() {}
    async metadata() {}
    async close() {}
  }
  return { Admin, Consumer, Producer, ProduceAcks: { ALL: -1 }, stringDeserializers: {}, stringSerializers: {} }
})

const { createKafkaEventBus } = await import('../src/kafka.ts')

const logger: Logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), fatal: vi.fn() }

afterEach(() => {
  vi.useRealTimers()
  kafka.consumers.length = 0
  kafka.failConsume = 0
})

async function subscribed(onFatal = vi.fn()) {
  const bus = await createKafkaEventBus({ brokers: ['kafka:9092'], clientId: 'svc', logger, onFatal })
  const subscription = await bus.subscribe({ groupId: 'svc', topics: ['orders.v1'], handler: async () => {} })
  return { bus, subscription, onFatal }
}

describe('a Kafka consumer that fails', () => {
  it('is closed and started again after a wait, instead of crashing the process (a failed rejoin after a rebalance)', async () => {
    vi.useFakeTimers()
    const { subscription, onFatal } = await subscribed()
    const [first] = kafka.consumers
    // What @platformatic/kafka emits when it can't rejoin the group: without a listener, an uncaught exception.
    expect(() => first.emit('error', new Error('syncGroup failed 4 times'))).not.toThrow()
    await vi.advanceTimersByTimeAsync(0)
    expect(first.closed).toBe(true)
    expect(first.stream.closed).toBe(true)
    expect(kafka.consumers).toHaveLength(1)

    await vi.advanceTimersByTimeAsync(1_000)
    expect(kafka.consumers).toHaveLength(2)
    expect(onFatal).not.toHaveBeenCalled()

    // A second failure while restarting is the same failure: one new consumer, not two.
    const [, second] = kafka.consumers
    second.stream.emit('error', new Error('connection reset'))
    second.emit('error', new Error('connection reset'))
    await vi.advanceTimersByTimeAsync(1_000)
    expect(kafka.consumers).toHaveLength(3)
    await subscription.close()
  })

  it('calls onFatal only when it can not start again for about 5 minutes', async () => {
    vi.useFakeTimers()
    const { subscription, onFatal } = await subscribed()
    kafka.failConsume = 1_000
    kafka.consumers[0].emit('error', new Error('syncGroup failed 4 times'))
    // Waits of 1, 2, 4, 8, 16, then 30 s: the 14th failed attempt is at 301 s.
    await vi.advanceTimersByTimeAsync(4 * 60_000)
    expect(onFatal).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(2 * 60_000)
    expect(onFatal).toHaveBeenCalledTimes(1)
    await subscription.close()
  })

  it('is not started again once the subscription is closed', async () => {
    vi.useFakeTimers()
    const { subscription } = await subscribed()
    await subscription.close()
    await vi.advanceTimersByTimeAsync(60_000)
    expect(kafka.consumers).toHaveLength(1)
    expect(kafka.consumers[0].closed).toBe(true)
  })
})
