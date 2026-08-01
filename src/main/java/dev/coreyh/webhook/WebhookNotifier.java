package dev.coreyh.webhook;

import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Posts alerts to a webhook without ever taking down the application that is
 * sending them.
 *
 * <p>Alerting is the code most likely to be written carelessly, because it is
 * not the product. It is also the code most likely to run while something is
 * already going wrong. A notifier that throws, blocks, or holds the process
 * open turns a small incident into an outage, so every failure here is
 * contained:
 *
 * <ul>
 *   <li>{@link #send} never throws and never blocks the calling thread.</li>
 *   <li>The HTTP client is built on first use, not at construction. Building
 *       one can fail in restricted environments, and that failure should not
 *       take out whatever was constructing the notifier.</li>
 *   <li>If the client cannot be built, the notifier disables itself for the
 *       rest of the session instead of retrying on every call.</li>
 *   <li>Worker threads are daemons, so a hung webhook cannot keep the JVM
 *       alive at shutdown.</li>
 *   <li>The send queue is bounded. A webhook endpoint that stops responding
 *       causes alerts to be dropped and counted, not accumulated until the
 *       process runs out of memory.</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * WebhookNotifier notifier = WebhookNotifier.builder()
 *         .url("https://example.com/hooks/abc123")
 *         .build();
 *
 * notifier.send("{\"text\":\"deploy finished\"}");
 * }</pre>
 *
 * <p>When the feature is switched off in config, use {@link #disabled()} and
 * keep the call sites unchanged. Every method on a disabled notifier is a
 * no-op, so callers never need a null check or an {@code if (enabled)}.
 *
 * <p>Instances are thread safe.
 */
public final class WebhookNotifier implements AutoCloseable {

    /**
     * How a body actually gets sent. Exists so the queueing, counting and
     * shutdown behaviour can be tested without a network: some hardened
     * runtimes will not create the NIO selector {@link HttpClient} needs, and a
     * library that cannot be exercised there is a library with a missing seam.
     * Production always uses {@link HttpTransport}.
     */
    interface Transport {
        /** @return the HTTP status code. Throws if the request could not be made. */
        int post(URI url, String body, Duration timeout) throws Exception;

        /** False once the transport has decided it cannot work at all. */
        default boolean usable() {
            return true;
        }
    }

    private final URI url;
    private final Duration timeout;
    private final ThreadPoolExecutor executor;
    private final PrintStream log;
    private final boolean enabled;
    private final Transport transport;

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();

    private WebhookNotifier(Builder b) {
        this.enabled = b.url != null && !b.url.isBlank();
        this.url = enabled ? URI.create(b.url) : null;
        this.timeout = b.timeout;
        this.log = b.log;
        this.transport = (b.transport != null) ? b.transport : new HttpTransport(this::write);

        if (!enabled) {
            this.executor = null;
            return;
        }
        // Bounded queue with a caller-visible drop policy. An unbounded queue
        // would turn an unreachable webhook into a memory leak, and
        // CallerRunsPolicy would push the blocking back onto the application
        // thread, which is the one thing this class exists to prevent.
        this.executor = new ThreadPoolExecutor(
                1, b.maxThreads,
                30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(b.queueCapacity),
                runnable -> {
                    Thread t = new Thread(runnable, "webhook-notifier");
                    t.setDaemon(true);
                    return t;
                },
                (runnable, ex) -> {
                    long n = dropped.incrementAndGet();
                    // Logged only on the first drop and then every 100th, so a
                    // dead endpoint cannot turn one problem into a flood of log
                    // lines about the problem.
                    if (n == 1 || n % 100 == 0) {
                        write("queue full, dropped " + n + " notification(s)");
                    }
                    // Thrown so send() can report false. Counting here and
                    // returning quietly would let send() fall through and
                    // report the message as accepted when it was discarded.
                    throw new RejectedExecutionException("webhook queue full");
                });
        this.executor.allowCoreThreadTimeOut(true);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A notifier that accepts calls and does nothing. */
    public static WebhookNotifier disabled() {
        return new Builder().build();
    }

    public boolean isEnabled() {
        return enabled && transport.usable();
    }

    /**
     * Queues a POST of {@code jsonBody}. Returns immediately.
     *
     * @return true if the message was accepted onto the queue, false if the
     *         notifier is disabled or the queue was full. A true return means
     *         accepted, not delivered; delivery is asynchronous and its outcome
     *         is reflected in {@link #stats()}.
     */
    public boolean send(String jsonBody) {
        if (!enabled || jsonBody == null || !transport.usable()) {
            return false;
        }
        long before = dropped.get();
        try {
            executor.execute(() -> post(jsonBody));
        } catch (RuntimeException e) {
            // The rejection handler already counted this. Anything else that
            // lands here (a shutdown race, for example) is counted too, because
            // silently losing an alert is worse than an inaccurate counter.
            if (dropped.get() == before) {
                dropped.incrementAndGet();
            }
            return false;
        }
        accepted.incrementAndGet();
        return true;
    }

    private void post(String jsonBody) {
        try {
            int status = transport.post(url, jsonBody, timeout);
            if (status / 100 == 2) {
                delivered.incrementAndGet();
            } else {
                failed.incrementAndGet();
                write("webhook returned HTTP " + status);
            }
        } catch (Exception e) {
            // Deliberately not retried. A retry on an alert path amplifies load
            // on an endpoint that is already struggling, and a late duplicate
            // alert is usually worse than a missing one. Callers that need
            // delivery guarantees want a queue, not a notifier.
            failed.incrementAndGet();
            write("webhook post failed: " + e);
        } catch (Throwable t) {
            // A worker thread dying quietly would silently stop all future
            // alerts, so even Errors are counted and swallowed here.
            failed.incrementAndGet();
            write("webhook post failed hard: " + t);
        }
    }

    /**
     * The real transport. Builds its {@link HttpClient} on first use rather
     * than at construction, because building one can fail outright in
     * restricted environments and that failure should not propagate to whoever
     * was merely creating a notifier.
     */
    static final class HttpTransport implements Transport {

        private final Consumer<String> log;
        /** Volatile with a synchronized slow path: only the first caller locks. */
        private volatile HttpClient client;
        private volatile boolean unavailable;

        HttpTransport(Consumer<String> log) {
            this.log = log;
        }

        @Override
        public boolean usable() {
            return !unavailable;
        }

        @Override
        public int post(URI url, String body, Duration timeout) throws Exception {
            HttpClient c = client(timeout);
            if (c == null) {
                throw new IllegalStateException("HTTP client unavailable");
            }
            HttpRequest request = HttpRequest.newBuilder(url)
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();
            return c.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        }

        private HttpClient client(Duration timeout) {
            HttpClient local = client;
            if (local != null) {
                return local;
            }
            if (unavailable) {
                return null;
            }
            synchronized (this) {
                if (client == null && !unavailable) {
                    try {
                        client = HttpClient.newBuilder().connectTimeout(timeout).build();
                    } catch (RuntimeException | Error e) {
                        // Latched, not retried: if the runtime cannot give us a
                        // client once, it will not on the next alert either, and
                        // retrying per call would add a failure to every send.
                        unavailable = true;
                        log.accept("HTTP client unavailable, notifications off for this session: " + e);
                    }
                }
                return client;
            }
        }
    }

    /** Counters for the life of this notifier. */
    public Stats stats() {
        return new Stats(accepted.get(), delivered.get(), failed.get(), dropped.get());
    }

    /**
     * Stops accepting new work and waits briefly for in-flight posts.
     *
     * <p>Bounded on purpose: shutdown should not stall because an alert
     * endpoint is unresponsive. The threads are daemons, so anything still
     * running when this returns cannot hold the JVM open.
     */
    @Override
    public void close() {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void write(String message) {
        if (log != null) {
            log.println("[webhook] " + message);
        }
    }

    /** Immutable snapshot of delivery counters. */
    public static final class Stats {
        public final long accepted;
        public final long delivered;
        public final long failed;
        public final long dropped;

        Stats(long accepted, long delivered, long failed, long dropped) {
            this.accepted = accepted;
            this.delivered = delivered;
            this.failed = failed;
            this.dropped = dropped;
        }

        @Override
        public String toString() {
            return "accepted=" + accepted + " delivered=" + delivered
                    + " failed=" + failed + " dropped=" + dropped;
        }
    }

    public static final class Builder {
        private String url;
        private Duration timeout = Duration.ofSeconds(10);
        private int queueCapacity = 256;
        private int maxThreads = 2;
        private PrintStream log = System.err;
        private Transport transport;

        /** Test seam. Production uses the real HTTP transport. */
        Builder transport(Transport transport) {
            this.transport = transport;
            return this;
        }

        /** A blank or null URL produces a disabled notifier rather than an error. */
        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        /** Messages held while workers are busy. Beyond this, sends are dropped. */
        public Builder queueCapacity(int queueCapacity) {
            if (queueCapacity < 1) {
                throw new IllegalArgumentException("queueCapacity must be at least 1");
            }
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder maxThreads(int maxThreads) {
            if (maxThreads < 1) {
                throw new IllegalArgumentException("maxThreads must be at least 1");
            }
            this.maxThreads = maxThreads;
            return this;
        }

        /** Set to null to silence the notifier's own diagnostics. */
        public Builder logTo(PrintStream log) {
            this.log = log;
            return this;
        }

        public WebhookNotifier build() {
            return new WebhookNotifier(this);
        }
    }
}
