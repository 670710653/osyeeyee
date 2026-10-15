import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicReference;

public final class FileTransferTests {
    private static final DateTimeFormatter RUN_ID_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final String RESULT_HEADER =
            "timestamp_utc,test_case,repetition,mode,workers,parts,size_bytes,sha256,elapsed_seconds,"
                    + "throughput_mb_s,status,destination,error\n";

    private record TestCase(String name, long size, String fileName) {
    }

    private static final class Measurements {
        long bytes;
        double seconds;
        int runs;
    }

    private FileTransferTests() {
    }

    public static void main(String[] args) throws Exception {
        Path workspace = TransferRunLog.workspaceDirectory();
        Path logDirectory = workspace.resolve("logs");
        Path logPath = TransferRunLog.defaultLogPath();
        for (int index = 0; index < args.length; index++) {
            if (args[index].equals("--log") && index + 1 < args.length) {
                logPath = workspace.resolve(args[++index]).normalize();
                if (!logPath.startsWith(workspace)) {
                    throw new IllegalArgumentException("Test logs must be saved inside the workspace");
                }
            } else {
                throw new IllegalArgumentException("Usage: java FileTransferTests [--log CSV_PATH]");
            }
        }
        Path absoluteLogPath = logPath.toAbsolutePath().normalize();
        Files.createDirectories(logDirectory);
        Path testRunsDirectory = Files.createDirectories(logDirectory.resolve("test_runs"));
        Path runDirectory = Files.createTempDirectory(
                testRunsDirectory, "run-" + RUN_ID_FORMAT.format(Instant.now()) + "-");
        Path shared = Files.createDirectories(runDirectory.resolve("shared"));
        Path downloads = Files.createDirectories(runDirectory.resolve("downloads"));
        Path testResults = runDirectory.resolve("test_results.csv");
        Files.writeString(testResults, RESULT_HEADER, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        List<TestCase> cases = List.of(
                new TestCase("empty file", 0, "empty.dat"),
                new TestCase("small file with spaces", 73, "small test file.bin"),
                new TestCase("10-part boundary", 10_003, "ten-part-boundary.bin"),
                new TestCase("1 MiB binary", 1L * 1024 * 1024, "one-megabyte.bin"),
                new TestCase("8 MiB binary", 8L * 1024 * 1024, "eight-megabyte.bin"));
        int completed = 0;
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        try (FileTransferServer server = new FileTransferServer(shared, 0, 64)) {
            int port = server.start();
            Thread serverThread = new Thread(() -> {
                try {
                    server.serve();
                } catch (IOException e) {
                    if (serverFailure.get() == null) {
                        serverFailure.set(e);
                    }
                }
            }, "file-transfer-test-server");
            serverThread.setDaemon(true);
            serverThread.start();

            List<String> configurations = List.of("traditional:1", "traditional:20", "nio:1", "nio:20");
            var measurements = new java.util.LinkedHashMap<String, Measurements>();
            for (TestCase testCase : cases) {
                Path source = shared.resolve(testCase.fileName());
                createFile(source, testCase.size(), 0x5eedL + testCase.size());
                String expectedHash = sha256(source);
                for (String configuration : configurations) {
                    String[] fields = configuration.split(":");
                    TransferProtocol.Mode mode = TransferProtocol.Mode.parse(fields[0]);
                    int workers = Integer.parseInt(fields[1]);
                    String key = testCase.name() + " | " + mode + " | " + workers;
                    Measurements group = measurements.computeIfAbsent(key, ignored -> new Measurements());
                    for (int repetition = 1; repetition <= 3; repetition++) {
                        Path destination = downloads.resolve(
                                testCase.fileName() + "." + mode + ".w" + workers + ".run" + repetition);
                        FileTransferClient.DownloadResult result = null;
                        String status = "PASS";
                        String error = "";
                        try {
                            result = FileTransferClient.download(
                                    "127.0.0.1", port, testCase.fileName(), destination, workers, mode, absoluteLogPath);
                            if (result.size() != testCase.size() || Files.size(destination) != testCase.size()) {
                                throw new AssertionError("Size verification failed for " + key);
                            }
                            if (!expectedHash.equals(result.sha256()) || !expectedHash.equals(sha256(destination))) {
                                throw new AssertionError("SHA-256 verification failed for " + key);
                            }
                        } catch (IOException | InterruptedException | RuntimeException | AssertionError failure) {
                            status = "FAIL";
                            error = failure.getMessage();
                            appendTestResult(testResults, testCase.name(), repetition, mode, workers,
                                    result == null ? -1 : result.size(),
                                    result == null ? "" : result.sha256(),
                                    result == null ? 0 : result.seconds(), status, destination, error);
                            throw failure;
                        }
                        appendTestResult(testResults, testCase.name(), repetition, mode, workers,
                                result.size(), result.sha256(), result.seconds(), status, destination, error);
                        group.bytes += result.size();
                        group.seconds += result.seconds();
                        group.runs++;
                        completed++;
                    }
                }
            }

            System.out.println("PASS: all " + completed + " transfers passed size and SHA-256 checks.");
            System.out.println("Per-transfer size, SHA-256, throughput, and status appended to "
                    + absoluteLogPath);
            System.out.println("Generated test files and per-run CSV report are in " + runDirectory);
            System.out.println("Average transfer and merge throughput (MB/s; localhost):");
            System.out.printf("%-28s %-13s %7s %12s %12s%n",
                    "Test case", "Mode", "Workers", "Avg seconds", "Avg MB/s");
            for (var entry : measurements.entrySet()) {
                String[] fields = entry.getKey().split(" \\| ");
                Measurements group = entry.getValue();
                double averageSeconds = group.seconds / group.runs;
                String throughput = group.bytes == 0 ? "n/a"
                        : String.format("%.3f", group.bytes / group.seconds / 1_000_000.0);
                System.out.printf("%-28s %-13s %7s %12.6f %12s%n",
                        fields[0], fields[1], fields[2], averageSeconds, throughput);
            }
            if (serverFailure.get() != null) {
                throw new IOException("Test server failed", serverFailure.get());
            }
        }
    }

    private static void appendTestResult(
            Path report, String testCase, int repetition, TransferProtocol.Mode mode, int workers,
            long size, String hash, double seconds, String status, Path destination, String error)
            throws IOException {
        String throughput = size >= 0 && seconds > 0
                ? String.format(Locale.ROOT, "%.6f", size / seconds / 1_000_000.0) : "";
        String row = csv(Instant.now().toString()) + "," + csv(testCase) + "," + repetition + ","
                + mode.name() + "," + workers + "," + TransferProtocol.PARTS + ","
                + (size >= 0 ? size : "") + "," + csv(hash) + ","
                + String.format(Locale.ROOT, "%.9f", seconds) + "," + throughput + ","
                + status + "," + csv(destination.toAbsolutePath().normalize().toString()) + ","
                + csv(error) + "\n";
        Files.writeString(report, row, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static String csv(String value) {
        String safeValue = value == null ? "" : value;
        return "\"" + safeValue.replace("\"", "\"\"").replace("\r", " ").replace("\n", " ") + "\"";
    }

    private static void createFile(Path path, long size, long seed) throws IOException {
        Random random = new Random(seed);
        byte[] buffer = new byte[64 * 1024];
        try (OutputStream output = Files.newOutputStream(path)) {
            long remaining = size;
            while (remaining > 0) {
                random.nextBytes(buffer);
                int count = (int) Math.min(buffer.length, remaining);
                output.write(buffer, 0, count);
                remaining -= count;
            }
        }
    }

    private static String sha256(Path file) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(file)) {
                byte[] buffer = new byte[64 * 1024];
                int count;
                while ((count = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, count);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

}
