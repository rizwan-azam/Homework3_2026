package homework3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Continuous workload: one worker commits (or retries) a transaction, then immediately starts
 * another. Thus the arrival/completion equilibrium is bounded by the number of workers; no
 * unbounded queue or accumulating unfinished transactions exists.
 */
final class Benchmark {
    private static final String POINT = "SELECT data_value FROM blocking_data WHERE data_id = ?";
    private static final String INSERT = "INSERT INTO blocking_data (data_id, data_value, modified_by) VALUES (?, ?, ?)";
    private static final String SCAN = "SELECT sum(data_value) FROM blocking_data";
    private static final String UPDATE = "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?";
    private static final String CSV_HEADER = "timestamp,mode,threads,rows,requested_seconds,max_transactions,seed," +
            "actual_seconds,committed,throughput_per_second,point_reads,inserts,full_scans,writes," +
            "retry_attempts,deadlocks,serialization_failures,lock_not_available,avg_latency_ms,max_latency_ms";

    private final Config cfg;
    private final InsertedRows insertedRows;
    private final WriteReservations writeReservations = new WriteReservations();
    private final AtomicLong issued = new AtomicLong();
    private final LongAdder committed = new LongAdder();
    private final LongAdder pointReads = new LongAdder();
    private final LongAdder inserts = new LongAdder();
    private final LongAdder scans = new LongAdder();
    private final LongAdder writes = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final LongAdder deadlocks = new LongAdder();
    private final LongAdder serialization = new LongAdder();
    private final LongAdder locks = new LongAdder();
    private final LongAdder totalLatencyNs = new LongAdder();
    private final AtomicLong maxLatencyNs = new AtomicLong();
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private volatile long startNs;
    private volatile long deadlineNs;

    Benchmark(Config cfg) {
        this.cfg = cfg;
        this.insertedRows = new InsertedRows(cfg.rows());
    }

    void execute() throws Exception {
        List<Worker> workers = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(cfg.threads());
        try {
            // Connections and statement preparation are outside the timed section.
            for (int i = 0; i < cfg.threads(); i++) workers.add(new Worker(i));
            CountDownLatch ready = new CountDownLatch(cfg.threads());
            CountDownLatch startSignal = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (Worker w : workers) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    try {
                        startSignal.await();
                        w.loop();
                    } catch (Throwable e) {
                        error.compareAndSet(null, e);
                    } finally {
                        try { w.close(); } catch (Exception e) { error.compareAndSet(null, e); }
                    }
                }));
            }
            ready.await();
            startNs = System.nanoTime();
            deadlineNs = cfg.durationSeconds() > 0
                    ? startNs + TimeUnit.SECONDS.toNanos(cfg.durationSeconds())
                    : Long.MAX_VALUE;
            startSignal.countDown();
            // The tasks have already been submitted; shutdown rejects new pool submissions,
            // but workers continue generating transactions internally until the deadline.
            pool.shutdown();
            long lastProgressBucket = 0;
            while (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
                if (error.get() != null) {
                    pool.shutdownNow();
                    break;
                }
                long elapsed = System.nanoTime() - startNs;
                long seconds = TimeUnit.NANOSECONDS.toSeconds(elapsed);
                long bucket = seconds / 5;
                if (bucket > lastProgressBucket) {
                    lastProgressBucket = bucket;
                    System.err.printf(Locale.ROOT, "[progress] %.1f s, %,d committed, %.2f tx/s%n",
                            elapsed / 1e9, committed.sum(), committed.sum() / (elapsed / 1e9));
                }
                // Stop admitting new plans at the deadline, but do not discard an
                // admitted transaction just because its drain takes a long time.
                // Progress output continues while waiting for database completion.
            }
            // Future completion is already reflected in the terminated executor; capture
            // time promptly (no extra one-second polling/sleep overhead in throughput).
            long endNs = System.nanoTime();
            for (Future<?> f : futures) f.get();
            if (error.get() != null) throw new IllegalStateException("Benchmark failed; no CSV recorded", error.get());
            double elapsedSeconds = (endNs - startNs) / 1e9;
            printAndStore(elapsedSeconds);
        } finally {
            pool.shutdownNow();
            // If worker construction failed before submission, close unopened workers too.
            for (Worker w : workers) try { w.close(); } catch (Exception ignored) { }
        }
    }

    /**
     * Atomically reserve all row IDs needed by one writer. Overlapping writers
     * wait, while disjoint writers and MVCC readers can still run concurrently.
     * Earlier conflicting waiters cannot be overtaken by later arrivals.
     *
     * For this benchmark's schema and one running benchmark process, this removes
     * writer deadlock cycles without changing UPDATE order. Progress assumes
     * finite database operations and runnable threads eventually being scheduled.
     * Outside applications and permanent database errors are not controlled here.
     * Prepared with OpenAI ChatGPT assistance.
     */
    static final class WriteReservations {
        private final Set<Integer> held = new HashSet<>();
        private final List<Request> waiting = new ArrayList<>();

        private static final class Request {
            final int[] keys;
            Request(int[] keys) { this.keys = keys.clone(); }
        }

        final class Lease implements AutoCloseable {
            private final Request request;
            private boolean released;
            private Lease(Request request) { this.request = request; }
            @Override public void close() {
                synchronized (WriteReservations.this) {
                    if (!released) {
                        for (int key : request.keys) held.remove(key);
                        released = true;
                        WriteReservations.this.notifyAll();
                    }
                }
            }
        }

        synchronized Lease acquire(int[] keys) throws InterruptedException {
            Request request = new Request(keys);
            Lease lease = new Lease(request);
            waiting.add(request);
            try {
                while (!canStart(request)) wait();
                waiting.remove(request);
                for (int key : request.keys) held.add(key);
                return lease;
            } catch (InterruptedException e) {
                waiting.remove(request);
                notifyAll();
                throw e;
            }
        }

        private boolean canStart(Request request) {
            for (int key : request.keys) if (held.contains(key)) return false;
            for (Request earlier : waiting) {
                if (earlier == request) break;
                for (int a : earlier.keys) {
                    for (int b : request.keys) if (a == b) return false;
                }
            }
            return true;
        }
    }

    private final class Worker implements AutoCloseable {
        private final int workerId;
        private final Connection c;
        private final EnumMap<Workload.Kind, PreparedStatement> cache = new EnumMap<>(Workload.Kind.class);
        private final java.sql.Statement multiStatement;

        Worker(int workerId) throws SQLException {
            this.workerId = workerId;
            c = Database.open(cfg);
            c.setAutoCommit(false);
            // READ COMMITTED provides PostgreSQL MVCC reads and row-level write conflicts.
            // SERIALIZABLE is not a requirement here and can add serialization failures.
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            if (cfg.mode().equals("optimized") || cfg.mode().equals("cached")) {
                // Cached mode reuses one PreparedStatement per SQL query kind, with normal
                // individual UPDATE execution and default PostgreSQL server-prepare settings.
                // Legacy experimental variant: JDBC batching with client-side cached statements.
                for (Workload.Kind k : Workload.Kind.values()) cache.put(k, c.prepareStatement(sql(k)));
            }
            if (cfg.mode().equals("roundtrip")) {
                // The candidate variant reuses prepared statements for the unchanged one-statement
                // INSERT/SCAN transactions, while sending the three independent ordered SELECT
                // or UPDATE statements in one JDBC execute() call.
                cache.put(Workload.Kind.INSERT, c.prepareStatement(INSERT));
                cache.put(Workload.Kind.FULL_SCAN, c.prepareStatement(SCAN));
                multiStatement = c.createStatement();
            } else {
                multiStatement = null;
            }
        }

        void loop() throws SQLException, InterruptedException {
            while (error.get() == null && System.nanoTime() < deadlineNs) {
                long id = issued.getAndIncrement();
                if (cfg.transactions() > 0 && id >= cfg.transactions()) break;
                Workload.Tx tx = Workload.make(id, cfg.seed(), cfg.rows(), insertedRows);
                long began = System.nanoTime();
                // All modes use the same scheduler, including the serial baseline.
                // Reserve the complete write set BEFORE the first SQL update.
                // Acquisition, key checking and waiting are inside measured runtime.
                try (WriteReservations.Lease lease = tx.kind() == Workload.Kind.WRITE
                        ? writeReservations.acquire(tx.dataIds()) : null) {
                    int attempt = 0;
                    while (true) { // A started transaction MUST eventually commit or fail visibly.
                        try {
                            double result = runStatements(tx);
                            c.commit();
                            // Publish the INSERT only after commit; other workers can then sample it.
                            if (tx.kind() == Workload.Kind.INSERT) {
                                insertedRows.publishedCommit(tx.insertionId());
                            }
                            long latency = System.nanoTime() - began;
                            totalLatencyNs.add(latency);
                            maxLatencyNs.accumulateAndGet(latency, Math::max);
                            switch (tx.kind()) {
                                case POINT_READ -> pointReads.increment();
                                case INSERT -> inserts.increment();
                                case FULL_SCAN -> scans.increment();
                                case WRITE -> writes.increment();
                            }
                            committed.increment();
                            if (cfg.printReads() && (tx.kind() == Workload.Kind.POINT_READ
                                    || tx.kind() == Workload.Kind.FULL_SCAN)) {
                                System.out.printf(Locale.ROOT, "COMMIT tx=%d worker=%d type=%s sum=%.6f%n",
                                        tx.id(), workerId, tx.kind(), result);
                            }
                            break;
                        } catch (SQLException ex) {
                            try { c.rollback(); } catch (SQLException rollbackError) {
                                ex.addSuppressed(rollbackError);
                                throw ex;
                            }
                            String sqlState = RetryPolicy.retryableState(ex);
                            if (sqlState == null) throw ex;
                            retries.increment();
                            if ("40P01".equals(sqlState)) deadlocks.increment();
                            if ("40001".equals(sqlState)) serialization.increment();
                            if ("55P03".equals(sqlState)) locks.increment();
                            attempt++;
                            // Backoff reduces repeated external conflicts; it is not a fairness guarantee.
                            RetryPolicy.backoff(attempt);
                            if (Thread.currentThread().isInterrupted()) {
                                throw new IllegalStateException("Worker interrupted while retrying", ex);
                            }
                        }
                    }
                } // Release only after this transaction has committed or rolled back.

            }
        }

        double runStatements(Workload.Tx tx) throws SQLException {
            if (multiStatement != null) {
                if (tx.kind() == Workload.Kind.POINT_READ) {
                    return RoundTrip.pointRead(multiStatement, tx.dataIds());
                }
                if (tx.kind() == Workload.Kind.WRITE) {
                    RoundTrip.updateThree(multiStatement, tx);
                    return Double.NaN;
                }
            }
            PreparedStatement ps = cache.get(tx.kind());
            boolean temporary = ps == null;
            if (temporary) ps = c.prepareStatement(sql(tx.kind()));
            try {
                switch (tx.kind()) {
                    case POINT_READ -> {
                        double sum = 0.0;
                        // Exactly 3 separate SELECT statements for 3 distinct existing tuples.
                        for (int key : tx.dataIds()) {
                            ps.setInt(1, key);
                            try (ResultSet rs = ps.executeQuery()) {
                                if (!rs.next()) throw new SQLException("Missing base tuple: " + key);
                                sum += rs.getDouble(1);
                            }
                        }
                        return sum;
                    }
                    case INSERT -> {
                        ps.setInt(1, tx.insertionId());
                        ps.setDouble(2, tx.values()[0]);
                        ps.setInt(3, Math.toIntExact(tx.id() % Integer.MAX_VALUE));
                        exactlyOne(ps.executeUpdate(), "INSERT");
                        return Double.NaN;
                    }
                    case FULL_SCAN -> {
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) throw new SQLException("SUM() returned no row");
                            return rs.getDouble(1);
                        }
                    }
                    case WRITE -> {
                        // Three distinct single-tuple UPDATE statements, in the original random
                        // order. Optimized mode batches them at the JDBC protocol layer to avoid
                        // three independent client/server round trips. JDBC keeps batch order.
                        if (cfg.mode().equals("optimized")) {
                            ps.clearBatch();
                            try {
                                for (int i = 0; i < 3; i++) {
                                    ps.setDouble(1, tx.values()[i]);
                                    ps.setInt(2, Math.toIntExact(tx.id() % Integer.MAX_VALUE));
                                    ps.setInt(3, tx.dataIds()[i]);
                                    ps.addBatch();
                                }
                                int[] counts = ps.executeBatch();
                                if (counts.length != 3) throw new SQLException("UPDATE batch count mismatch");
                                for (int changed : counts) exactlyOne(changed, "UPDATE");
                            } finally {
                                // Important after any failed attempt: never retry a stale batch.
                                ps.clearBatch();
                            }
                        } else {
                            for (int i = 0; i < 3; i++) {
                                ps.setDouble(1, tx.values()[i]);
                                ps.setInt(2, Math.toIntExact(tx.id() % Integer.MAX_VALUE));
                                ps.setInt(3, tx.dataIds()[i]);
                                exactlyOne(ps.executeUpdate(), "UPDATE");
                            }
                        }
                        return Double.NaN;
                    }
                }
                throw new IllegalStateException("Unknown kind");
            } finally {
                if (temporary) ps.close();
            }
        }

        @Override public void close() throws SQLException {
            try {
                if (multiStatement != null) multiStatement.close();
                for (PreparedStatement ps : cache.values()) ps.close();
            } finally {
                if (!c.isClosed()) c.close();
            }
        }
    }

    private static void exactlyOne(int affected, String operation) throws SQLException {
        if (affected != 1) throw new SQLException(operation + " changed " + affected + " rows (expected 1)");
    }

    private static String sql(Workload.Kind kind) {
        return switch (kind) {
            case POINT_READ -> POINT;
            case INSERT -> INSERT;
            case FULL_SCAN -> SCAN;
            case WRITE -> UPDATE;
        };
    }

    private void printAndStore(double elapsed) throws IOException {
        System.err.println("writerScheduling=atomic row reservations with conflict FIFO fairness");
        long count = committed.sum();
        double tps = count / elapsed;
        double avgLatencyMs = count > 0 ? totalLatencyNs.sum() / (double) count / 1e6 : 0.0;
        double maxLatencyMs = maxLatencyNs.get() / 1e6;
        System.err.printf(Locale.ROOT,
                """
                %n=== COMPLETED BENCHMARK ===
                mode=%s, threads=%d, baseRows=%d, elapsed=%.3fs (includes drain)
                committed=%d, throughput=%.2f transactions/s
                POINT_READ=%d (%.1f%%), INSERT=%d (%.1f%%), FULL_SCAN=%d (%.1f%%), WRITE=%d (%.1f%%)
                retries=%d, deadlocks=%d, serializationFailures=%d, lockUnavailable=%d
                avgTransactionLatency=%.3fms, maxTransactionLatency=%.3fms
                CSV: %s
                """,
                cfg.mode(), cfg.threads(), cfg.rows(), elapsed,
                count, tps,
                pointReads.sum(), pct(pointReads.sum(), count),
                inserts.sum(), pct(inserts.sum(), count),
                scans.sum(), pct(scans.sum(), count),
                writes.sum(), pct(writes.sum(), count),
                retries.sum(), deadlocks.sum(), serialization.sum(), locks.sum(),
                avgLatencyMs, maxLatencyMs, cfg.csvFile());
        Path csv = cfg.csvFile();
        if (csv.getParent() != null) Files.createDirectories(csv.getParent());
        if (!Files.exists(csv) || Files.size(csv) == 0) {
            Files.writeString(csv, CSV_HEADER + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        String record = String.format(Locale.ROOT,
                "%s,%s,%d,%d,%d,%d,%d,%.3f,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f%n",
                Instant.now(), cfg.mode(), cfg.threads(), cfg.rows(), cfg.durationSeconds(),
                cfg.transactions(), cfg.seed(), elapsed, count, tps,
                pointReads.sum(), inserts.sum(), scans.sum(), writes.sum(),
                retries.sum(), deadlocks.sum(), serialization.sum(), locks.sum(),
                avgLatencyMs, maxLatencyMs);
        Files.writeString(csv, record, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private static double pct(long a, long b) { return b == 0 ? 0 : 100.0 * a / b; }
}
