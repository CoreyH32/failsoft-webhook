# failsoft-webhook

Posts alerts to a webhook without ever taking down the application sending them.

No dependencies. One class. Java 11 or newer.

## Why

Alerting is the code most likely to be written carelessly, because it is not the
product. It is also the code most likely to run while something is already going
wrong. A notifier that throws, blocks, or holds the process open turns a small
incident into an outage.

I have hit all three: a notifier built at class-init that failed and took the
feature down with it, a synchronous post that stalled a request thread when the
endpoint got slow, and a non-daemon HTTP worker that kept the JVM from exiting
at shutdown.

## What it guarantees

- **`send` never throws and never blocks the caller.** It queues and returns.
- **The HTTP client is built on first use, not at construction.** Building one
  can fail in restricted environments, and that failure should not take out
  whatever was constructing the notifier.
- **If the client cannot be built, the notifier disables itself for the
  session** instead of retrying, and failing, on every subsequent call.
- **Workers are daemon threads,** so a hung webhook cannot keep the JVM alive
  at shutdown.
- **The queue is bounded.** An endpoint that stops responding causes alerts to
  be dropped and counted, never accumulated until the process runs out of
  memory.

```java
WebhookNotifier notifier = WebhookNotifier.builder()
        .url("https://example.com/hooks/abc123")
        .build();

notifier.send("{\"text\":\"deploy finished\"}");
```

Switched off in config? Use `WebhookNotifier.disabled()` and leave the call
sites alone. Every method becomes a no-op, so callers never need a null check.

Counters are available for a health endpoint:

```java
notifier.stats();   // accepted=41 delivered=39 failed=2 dropped=0
```

## Design notes

**Bounded queue with a drop policy, not `CallerRunsPolicy`.** Caller-runs would
push the blocking back onto the application thread, which is the one thing this
class exists to prevent. Excess is dropped and counted instead.

**Drops are logged on the first occurrence and then every hundredth.** A dead
endpoint should not turn one problem into a flood of log lines about the
problem.

**No retries.** Retrying on an alert path amplifies load on an endpoint that is
already struggling, and a late duplicate alert is usually worse than a missing
one. Code that needs delivery guarantees wants a queue, not a notifier.

**The client failure is latched, not retried.** If the runtime cannot provide an
HTTP client once, it will not provide one on the next alert either, and retrying
per call would add a failure to every future send.

**`close()` waits at most two seconds.** Shutdown should not stall because an
alert endpoint is unresponsive. The threads are daemons, so anything still
running when close returns cannot hold the JVM open.

**Errors are caught in the worker, not just exceptions.** A worker thread dying
quietly would silently stop all future alerts.

## Tests

```bash
javac -d out $(find src -name "*.java")
java -cp out dev.coreyh.webhook.WebhookSelfTest
```

The suite starts a real HTTP server on loopback, built on a plain
`ServerSocket` rather than `com.sun.net.httpserver`, and covers delivery, the
JSON content type, a 500 response, an endpoint that is not there, an endpoint
that hangs, queue saturation, and daemon threads.

Environments that refuse to create an NIO selector cannot construct a
`java.net.http.HttpClient` at all. The suite detects that and reports those
checks as SKIP rather than failing, since it is a property of the machine and
not of this code.

## Limitations

- No retry and no persistence. If the endpoint is down, the alert is gone.
- No rate limiting or de-duplication. If you alert in a loop, it posts in a
  loop. Gate that at the call site, or at the state-change level where the
  decision belongs.
- The body is passed through as-is. This does not build or validate JSON.

## License

MIT
