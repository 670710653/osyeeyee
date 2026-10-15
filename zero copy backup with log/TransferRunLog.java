import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.net.URISyntaxException;

final class TransferRunLog {
    private static final String HEADER = "timestamp_utc,host,port,filename,destination,mode,workers,parts,"
            + "size_bytes,sha256,elapsed_seconds,throughput_mb_s,status,error\n";
    private static final Object JVM_LOCK = new Object();

    private TransferRunLog() {
    }

    static Path workspaceDirectory() {
        try {
            Path location = Path.of(TransferRunLog.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI()).toAbsolutePath().normalize();
            return Files.isDirectory(location) ? location : location.getParent();
        } catch (URISyntaxException | NullPointerException e) {
            throw new IllegalStateException("Could not locate the application workspace", e);
        }
    }

    static Path defaultLogPath() {
        return workspaceDirectory().resolve("logs").resolve("transfer_metrics.csv");
    }

    static void append(
            Path logPath,
            String host,
            int port,
            String fileName,
            Path destination,
            TransferProtocol.Mode mode,
            int workers,
            long size,
            String sha256,
            double seconds,
            String status,
            String error) throws IOException {
        Path absoluteLog = logPath.toAbsolutePath().normalize();
        Path parent = absoluteLog.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String throughput = size >= 0 && seconds > 0
                ? String.format(java.util.Locale.ROOT, "%.6f", size / seconds / 1_000_000.0) : "";
        String row = csv(Instant.now().toString()) + "," + csv(host) + "," + port + ","
                + csv(fileName) + "," + csv(destination.toAbsolutePath().normalize().toString()) + ","
                + mode.name() + "," + workers + "," + TransferProtocol.PARTS + ","
                + (size >= 0 ? size : "") + "," + csv(sha256) + ","
                + String.format(java.util.Locale.ROOT, "%.9f", seconds) + "," + throughput + ","
                + csv(status) + "," + csv(error) + "\n";

        synchronized (JVM_LOCK) {
            try (FileChannel channel = FileChannel.open(absoluteLog, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
                 FileLock lock = channel.lock()) {
                if (!lock.isValid()) {
                    throw new IOException("Could not lock transfer log: " + absoluteLog);
                }
                if (channel.size() == 0) {
                    writeFully(channel, ByteBuffer.wrap(HEADER.getBytes(StandardCharsets.UTF_8)));
                }
                channel.position(channel.size());
                writeFully(channel, ByteBuffer.wrap(row.getBytes(StandardCharsets.UTF_8)));
                channel.force(true);
            }
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static String csv(String value) {
        String safeValue = value == null ? "" : value;
        return "\"" + safeValue.replace("\"", "\"\"").replace("\r", " ").replace("\n", " ") + "\"";
    }
}
