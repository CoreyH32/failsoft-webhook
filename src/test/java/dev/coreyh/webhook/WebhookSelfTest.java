package dev.coreyh.webhook;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Runs the notifier against a real HTTP server on loopback, including the
 * failure cases that matter: a server returning 500, a server that never
 * answers in time, and an endpoint that is not there at all.
 *
 * <p>The test server is a plain {@link ServerSocket} speaking just enough
 * HTTP/1.1 rather than {@code com.sun.net.httpserver}. That keeps the test off
 * a JDK-internal module, and it avoids needing an NIO selector, which some
 * hardened environments refuse to create.
 *
 * <pre>
 *   java -cp out dev.coreyh.webhook.WebhookSelfTest
 * </pre>
 */
public final class WebhookSelfTest {

    /** Checks skipped, rather than failed, when the runtime cannot do HTTP. */
    private static int skipped;

    /**
     * Some hardened environments refuse to create an NIO selector, which
     * {@link java.net.http.HttpClient} needs. That is a property of the machine,
     * not a defect in this library, so the network checks report SKIP instead of
     * failing the run.
     */
    private static boolean httpAvailable() {
        try {
            java.net.http.HttpClient.newHttpClient();
            return true;
        } catch (RuntimeException | Error e) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        int failures = 0;
        final boolean http = httpAvailable();
        if (!http) {
            System.out.println("NOTE: this runtime cannot create an HTTP client "
                    + "(no NIO selector). Network checks will be skipped.\n");
        }

        failures += net(http, "delivers a POST and reports it", () -> {
            try (TinyServer server = TinyServer.start(200, 0);
                 WebhookNotifier n = notifier(server).build()) {
                boolean accepted = n.send("{\"text\":\"hello\"}");
                boolean arrived = server.awaitRequests(1, 5_000);
                waitFor(() -> n.stats().delivered == 1, 3_000);
                return accepted && arrived
                        && server.bodies.get(0).contains("hello")
                        && n.stats().delivered == 1;
            }
        });

        failures += net(http, "sends the JSON content type", () -> {
            try (TinyServer server = TinyServer.start(200, 0);
                 WebhookNotifier n = notifier(server).build()) {
                n.send("{}");
                return server.awaitRequests(1, 5_000)
                        && server.headers.get(0).toLowerCase().contains("content-type: application/json");
            }
        });

        failures += net(http, "counts a 500 as failed and does not throw", () -> {
            try (TinyServer server = TinyServer.start(500, 0);
                 WebhookNotifier n = notifier(server).logTo(null).build()) {
                n.send("{}");
                server.awaitRequests(1, 5_000);
                waitFor(() -> n.stats().failed == 1, 3_000);
                WebhookNotifier.Stats s = n.stats();
                return s.failed == 1 && s.delivered == 0;
            }
        });

        failures += net(http, "a dead endpoint fails without throwing or blocking the caller", () -> {
            // Port 1 on loopback: nothing listens there.
            try (WebhookNotifier n = WebhookNotifier.builder()
                    .url("http://127.0.0.1:1/hook")
                    .timeout(Duration.ofMillis(500))
                    .logTo(null)
                    .build()) {
                long start = System.nanoTime();
                boolean accepted = n.send("{}");
                long callerMs = (System.nanoTime() - start) / 1_000_000;
                boolean counted = waitFor(() -> n.stats().failed == 1, 5_000);
                // The caller must return immediately even though delivery fails.
                return accepted && callerMs < 250 && counted;
            }
        });

        failures += net(http, "a hung endpoint times out instead of hanging forever", () -> {
            try (TinyServer server = TinyServer.start(200, 4_000); // holds each request
                 WebhookNotifier n = notifier(server).timeout(Duration.ofMillis(500))
                         .logTo(null).build()) {
                n.send("{}");
                boolean counted = waitFor(() -> n.stats().failed == 1, 5_000);
                return counted && n.stats().delivered == 0;
            }
        });

        failures += net(http, "drops instead of growing without bound when the queue fills", () -> {
            try (TinyServer server = TinyServer.start(200, 3_000); // slow, so work backs up
                 WebhookNotifier n = notifier(server)
                         .queueCapacity(4).maxThreads(1).logTo(null).build()) {
                for (int i = 0; i < 200; i++) {
                    n.send("{\"i\":" + i + "}");
                }
                WebhookNotifier.Stats s = n.stats();
                // The point: excess is dropped and counted, never queued.
                return s.dropped > 0 && s.accepted < 200 && (s.accepted + s.dropped) == 200;
            }
        });

        // ---- Transport-level checks. No network, so these run everywhere. ----

        failures += check("counts a 2xx as delivered", () -> {
            try (WebhookNotifier n = fake(() -> 200)) {
                boolean accepted = n.send("{}");
                return accepted && waitFor(() -> n.stats().delivered == 1, 3_000)
                        && n.stats().failed == 0;
            }
        });

        failures += check("counts a non-2xx as failed, without throwing", () -> {
            try (WebhookNotifier n = fake(() -> 503)) {
                n.send("{}");
                return waitFor(() -> n.stats().failed == 1, 3_000)
                        && n.stats().delivered == 0;
            }
        });

        failures += check("a throwing transport is contained, not propagated", () -> {
            try (WebhookNotifier n = WebhookNotifier.builder()
                    .url("http://example.invalid/hook").logTo(null)
                    .transport((u, b, t) -> {
                        throw new IllegalStateException("boom");
                    }).build()) {
                boolean accepted = n.send("{}"); // must not throw here
                return accepted && waitFor(() -> n.stats().failed == 1, 3_000);
            }
        });

        failures += check("send does not block on a slow transport", () -> {
            CountDownLatch hold = new CountDownLatch(1);
            try (WebhookNotifier n = WebhookNotifier.builder()
                    .url("http://example.invalid/hook").logTo(null)
                    .transport((u, b, t) -> {
                        hold.await(5, TimeUnit.SECONDS);
                        return 200;
                    }).build()) {
                long start = System.nanoTime();
                n.send("{}");
                long callerMs = (System.nanoTime() - start) / 1_000_000;
                return callerMs < 250;
            } finally {
                hold.countDown();
            }
        });

        failures += check("drops instead of queueing without bound", () -> {
            CountDownLatch hold = new CountDownLatch(1);
            try (WebhookNotifier n = WebhookNotifier.builder()
                    .url("http://example.invalid/hook").logTo(null)
                    .queueCapacity(4).maxThreads(1)
                    .transport((u, b, t) -> {
                        hold.await(10, TimeUnit.SECONDS);
                        return 200;
                    }).build()) {
                for (int i = 0; i < 200; i++) {
                    n.send("{\"i\":" + i + "}");
                }
                WebhookNotifier.Stats s = n.stats();
                // The point: excess is dropped and counted, never accumulated.
                return s.dropped > 0 && s.accepted < 200 && (s.accepted + s.dropped) == 200;
            } finally {
                hold.countDown();
            }
        });

        failures += check("worker threads are daemons", () -> {
            CountDownLatch hold = new CountDownLatch(1);
            try (WebhookNotifier n = WebhookNotifier.builder()
                    .url("http://example.invalid/hook").logTo(null)
                    .transport((u, b, t) -> {
                        hold.await(5, TimeUnit.SECONDS);
                        return 200;
                    }).build()) {
                n.send("{}");
                waitFor(() -> hasThread("webhook-notifier"), 3_000);
                return hasThread("webhook-notifier")
                        && Thread.getAllStackTraces().keySet().stream()
                                .filter(t -> t.getName().startsWith("webhook-notifier"))
                                .allMatch(Thread::isDaemon);
            } finally {
                hold.countDown();
            }
        });

        failures += check("an unusable transport disables sending", () -> {
            try (WebhookNotifier n = WebhookNotifier.builder()
                    .url("http://example.invalid/hook").logTo(null)
                    .transport(new WebhookNotifier.Transport() {
                        @Override public boolean usable() { return false; }
                        @Override public int post(java.net.URI u, String b, Duration t) {
                            throw new AssertionError("must not be called when unusable");
                        }
                    }).build()) {
                return !n.send("{}") && !n.isEnabled() && n.stats().accepted == 0;
            }
        });

        failures += check("a disabled notifier is a safe no-op", () -> {
            try (WebhookNotifier n = WebhookNotifier.disabled()) {
                return !n.send("{}") && !n.isEnabled() && n.stats().accepted == 0;
            }
        });

        failures += check("a blank url disables rather than erroring", () -> {
            try (WebhookNotifier n = WebhookNotifier.builder().url("   ").build()) {
                return !n.isEnabled() && !n.send("{}");
            }
        });

        failures += check("send after close does not throw", () -> {
            try (TinyServer server = TinyServer.start(200, 0)) {
                WebhookNotifier n = notifier(server).logTo(null).build();
                n.send("{}");
                n.close();
                n.send("{}"); // must not throw
                return true;
            }
        });

        failures += net(http, "worker threads are daemons", () -> {
            try (TinyServer server = TinyServer.start(200, 0);
                 WebhookNotifier n = notifier(server).logTo(null).build()) {
                n.send("{}");
                waitFor(() -> hasThread("webhook-notifier"), 3_000);
                return Thread.getAllStackTraces().keySet().stream()
                        .filter(t -> t.getName().startsWith("webhook-notifier"))
                        .allMatch(Thread::isDaemon)
                        && hasThread("webhook-notifier");
            }
        });

        System.out.println();
        if (failures > 0) {
            System.out.println("self-test: " + failures + " check(s) FAILED");
            System.exit(1);
        } else if (skipped > 0) {
            System.out.println("self-test: all runnable checks passed, "
                    + skipped + " skipped (no HTTP support on this runtime)");
            System.out.println("Delivery over HTTP is NOT verified here. Run this on a normal"
                    + " machine to exercise the network checks.");
        } else {
            System.out.println("self-test: all checks passed");
        }
    }

    /** A check that needs working HTTP. Reports SKIP rather than FAIL without it. */
    private static int net(boolean httpAvailable, String name, Check body) {
        if (!httpAvailable) {
            skipped++;
            System.out.println("SKIP: " + name);
            return 0;
        }
        return check(name, body);
    }

    private static boolean hasThread(String prefix) {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.getName().startsWith(prefix));
    }

    /** Polls until the condition holds, rather than sleeping a fixed guess. */
    private static boolean waitFor(Condition c, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (c.test()) {
                return true;
            }
            Thread.sleep(25);
        }
        return c.test();
    }

    /** A notifier whose transport returns a fixed status without any network. */
    private static WebhookNotifier fake(java.util.function.IntSupplier status) {
        return WebhookNotifier.builder()
                .url("http://example.invalid/hook")
                .logTo(null)
                .transport((u, b, t) -> status.getAsInt())
                .build();
    }

    private static WebhookNotifier.Builder notifier(TinyServer server) {
        return WebhookNotifier.builder()
                .url("http://127.0.0.1:" + server.port() + "/hook")
                .timeout(Duration.ofSeconds(3));
    }

    /** Minimal HTTP/1.1 server: enough to accept a POST and answer with a status. */
    static final class TinyServer implements AutoCloseable {
        private final ServerSocket socket;
        private final int status;
        private final long holdMs;
        private final Thread acceptor;
        private volatile boolean running = true;

        final List<String> bodies = new CopyOnWriteArrayList<>();
        final List<String> headers = new CopyOnWriteArrayList<>();
        private final CountDownLatch anyRequest = new CountDownLatch(1);

        static TinyServer start(int status, long holdMs) throws IOException {
            return new TinyServer(status, holdMs);
        }

        private TinyServer(int status, long holdMs) throws IOException {
            this.status = status;
            this.holdMs = holdMs;
            this.socket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            this.acceptor = new Thread(this::acceptLoop, "tiny-http");
            this.acceptor.setDaemon(true);
            this.acceptor.start();
        }

        int port() {
            return socket.getLocalPort();
        }

        boolean awaitRequests(int atLeast, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (bodies.size() >= atLeast) {
                    return true;
                }
                Thread.sleep(20);
            }
            return bodies.size() >= atLeast;
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket client = socket.accept();
                    Thread worker = new Thread(() -> handle(client), "tiny-http-conn");
                    worker.setDaemon(true);
                    worker.start();
                } catch (IOException e) {
                    return; // socket closed
                }
            }
        }

        private void handle(Socket client) {
            try (Socket c = client) {
                c.setSoTimeout(10_000);
                InputStream in = new BufferedInputStream(c.getInputStream());

                StringBuilder head = new StringBuilder();
                int b, consecutiveNewlines = 0;
                while ((b = in.read()) != -1) {
                    head.append((char) b);
                    if (b == '\n') {
                        if (++consecutiveNewlines == 2) {
                            break;
                        }
                    } else if (b != '\r') {
                        consecutiveNewlines = 0;
                    }
                }
                String rawHead = head.toString();
                headers.add(rawHead);

                int length = 0;
                for (String line : rawHead.split("\r?\n")) {
                    if (line.toLowerCase().startsWith("content-length:")) {
                        length = Integer.parseInt(line.substring(15).trim());
                    }
                }
                byte[] body = new byte[length];
                int read = 0;
                while (read < length) {
                    int n = in.read(body, read, length - read);
                    if (n < 0) {
                        break;
                    }
                    read += n;
                }
                bodies.add(new String(body, 0, Math.max(read, 0), StandardCharsets.UTF_8));
                anyRequest.countDown();

                if (holdMs > 0) {
                    Thread.sleep(holdMs);
                }

                OutputStream out = c.getOutputStream();
                out.write(("HTTP/1.1 " + status + " X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .getBytes(StandardCharsets.US_ASCII));
                out.flush();
            } catch (Exception ignored) {
                // Client hung up or timed out; nothing useful to do in a test server.
            }
        }

        @Override
        public void close() {
            running = false;
            try {
                socket.close();
            } catch (IOException ignored) {
                // already closed
            }
        }
    }

    private interface Condition {
        boolean test() throws Exception;
    }

    private interface Check {
        boolean run() throws Exception;
    }

    private static int check(String name, Check body) {
        try {
            if (body.run()) {
                System.out.println("PASS: " + name);
                return 0;
            }
            System.out.println("FAIL: " + name);
            return 1;
        } catch (Exception e) {
            System.out.println("FAIL: " + name + " threw " + e);
            return 1;
        }
    }
}
