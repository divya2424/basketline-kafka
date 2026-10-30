type LogFn = (details: object, message: string) => void

/** The logging the bus does; a pino logger fits. */
export interface Logger {
  info: LogFn
  warn: LogFn
  error: LogFn
  fatal: LogFn
}
