import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class FileTransferClient {
    public record DownloadResult(long size, String sha256, double seconds) {
        public double megabytesPerSecond() {
            return seconds == 0 ? 0 : size / seconds / 1_000_000.0;
        }
    }

    private record Range(long fileOffset, long length, int partIndex, long partOffset) {
    }

    private static final class TransferAttempt {
        long size = -1;
        String sha256 = "";
    }

    private FileTransferClient() {
    }

    public static DownloadResult download(
            String host, int port, String fileName, Path destination, int workers, TransferProtocol.Mode mode)
                throws IOException, InterruptedException {
            return download(host, port, fileName, destination, workers, mode, TransferRunLog.defaultLogPath());
    }

    public static DownloadResult download(
            String host, int port, String fileName, Path destination, int workers, TransferProtocol.Mode mode,
            Path logPath) throws IOException, InterruptedException {
        long started = System.nanoTime();
        TransferAttempt attempt = new TransferAttempt();
        try {
            DownloadResult result = downloadWithoutLogging(
                    host, port, fileName, destination, workers, mode, attempt);
            TransferRunLog.append(logPath, host, port, fileName, destination, mode, workers, result.size(),
                    result.sha256(), result.seconds(), "SUCCESS", "");
            return result;
        } catch (IOException | InterruptedException | RuntimeException e) {
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
            try {
                TransferRunLog.append(logPath, host, port, fileName, destination, mode, workers,
                        attempt.size, attempt.sha256, elapsed, "FAILED", e.getMessage());
            } catch (IOException logFailure) {
                e.addSuppressed(logFailure);
            }
            throw e;
        }
    }

    private static DownloadResult downloadWithoutLogging(
            String host, int port, String fileName, Path destination, int workers, TransferProtocol.Mode mode,
            TransferAttempt attempt)
            throws IOException, InterruptedException {
        if (workers < 1 || workers > 1024) {
            throw new IllegalArgumentException("Workers must be between 1 and 1024");
        }
        Path output = destination.toAbsolutePath().normalize();
        Path parent = output.getParent();
        if (parent == null) {
            throw new IOException("Output path must have a parent directory");
        }
        Files.createDirectories(parent);
        long size = getSize(host, port, fileName);
        attempt.size = size;
        String expectedHash = getHash(host, port, fileName);
        attempt.sha256 = expectedHash;
        Path temporaryDirectory = Files.createTempDirectory(parent, ".download-parts-");
        Path stagedOutput = Files.createTempFile(parent, ".download-staging-", ".tmp");
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        boolean completed = false;
        try {
            List<Path> parts = createParts(temporaryDirectory, size);
            List<Range> ranges = splitRanges(size, workers);
            List<Future<Void>> futures = new ArrayList<>(ranges.size());
            long started = System.nanoTime();
            for (Range range : ranges) {
                Path partPath = parts.get(range.partIndex());
                Callable<Void> task = () -> {
                    downloadRange(host, port, fileName, range, partPath, mode);
                    return null;
                };
                futures.add(pool.submit(task));
            }
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    for (Future<Void> pending : futures) {
                        pending.cancel(true);
                    }
                    Throwable cause = e.getCause();
                    if (cause instanceof IOException ioException) {
                        throw ioException;
                    }
                    throw new IOException("A download worker failed", cause);
                }
            }

            mergeParts(parts, stagedOutput, size, mode);
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            long actualSize = Files.size(stagedOutput);
            String actualHash = sha256(stagedOutput);
            if (actualSize != size) {
                throw new IOException("Downloaded size mismatch: expected " + size + ", got " + actualSize);
            }
            if (!expectedHash.equals(actualHash)) {
                throw new IOException("Downloaded SHA-256 mismatch: expected " + expectedHash + ", got "
                        + actualHash);
            }
            try {
                Files.move(stagedOutput, output, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(stagedOutput, output, StandardCopyOption.REPLACE_EXISTING);
            }
            completed = true;
            return new DownloadResult(size, actualHash, seconds);
        } finally {
            pool.shutdownNow();
            deleteTree(temporaryDirectory);
            if (!completed) {
                Files.deleteIfExists(stagedOutput);
            }
        }
    }

    private static List<Path> createParts(Path directory, long totalSize) throws IOException {
        List<Path> paths = new ArrayList<>(TransferProtocol.PARTS);
        long quotient = totalSize / TransferProtocol.PARTS;
        long remainder = totalSize % TransferProtocol.PARTS;
        for (int index = 0; index < TransferProtocol.PARTS; index++) {
            long partSize = quotient + (index < remainder ? 1 : 0);
            Path path = directory.resolve(String.format("part-%02d.bin", index));
            try (RandomAccessFile part = new RandomAccessFile(path.toFile(), "rw")) {
                part.setLength(partSize);
            }
            paths.add(path);
        }
        return paths;
    }

    private static List<Range> splitRanges(long totalSize, int workers) {
        int splitsPerPart = Math.max(1, (workers + TransferProtocol.PARTS - 1) / TransferProtocol.PARTS);
        List<Range> ranges = new ArrayList<>();
        long partStart = 0;
        long quotient = totalSize / TransferProtocol.PARTS;
        long remainder = totalSize % TransferProtocol.PARTS;
        for (int part = 0; part < TransferProtocol.PARTS; part++) {
            long partSize = quotient + (part < remainder ? 1 : 0);
            long sliceSize = partSize / splitsPerPart;
            long extra = partSize % splitsPerPart;
            long localOffset = 0;
            for (int split = 0; split < splitsPerPart; split++) {
                long length = sliceSize + (split < extra ? 1 : 0);
                if (length > 0) {
                    ranges.add(new Range(partStart + localOffset, length, part, localOffset));
                }
                localOffset += length;
            }
            partStart += partSize;
        }
        return ranges;
    }

    private static void downloadRange(
            String host, int port, String name, Range range, Path part, TransferProtocol.Mode mode)
            throws IOException {
        try (SocketChannel channel = TransferProtocol.connect(host, port)) {
            TransferProtocol.writeLine(channel, "GET " + TransferProtocol.encodeName(name) + " "
                    + range.fileOffset() + " " + range.length() + " " + mode.name());
            String response = TransferProtocol.requireOk(TransferProtocol.readLine(channel));
            String[] fields = response.split(" ");
            if (fields.length != 2 || !fields[0].equals("OK")
                    || Long.parseLong(fields[1]) != range.length()) {
                throw new IOException("Unexpected range response: " + response);
            }
            if (mode == TransferProtocol.Mode.nio) {
                receiveWithTransferFrom(channel, part, range.partOffset(), range.length());
            } else {
                receiveWithTraditionalIo(channel, part, range.partOffset(), range.length());
            }
        }
    }

    private static void receiveWithTraditionalIo(
            SocketChannel channel, Path part, long offset, long length) throws IOException {
        try (InputStream input = channel.socket().getInputStream();
             RandomAccessFile output = new RandomAccessFile(part.toFile(), "rw")) {
            output.seek(offset);
            byte[] buffer = new byte[64 * 1024];
            long remaining = length;
            while (remaining > 0) {
                int count = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (count < 0) {
                    throw new IOException("Server closed before sending the requested range");
                }
                output.write(buffer, 0, count);
                remaining -= count;
            }
        }
    }

    private static void receiveWithTransferFrom(
            SocketChannel channel, Path part, long offset, long length) throws IOException {
        try (FileChannel output = FileChannel.open(part, StandardOpenOption.WRITE)) {
            long position = offset;
            long remaining = length;
            ByteBuffer fallback = ByteBuffer.allocateDirect(64 * 1024);
            while (remaining > 0) {
                long received = output.transferFrom(channel, position, remaining);
                if (received > 0) {
                    position += received;
                    remaining -= received;
                    continue;
                }
                fallback.clear();
                fallback.limit((int) Math.min(fallback.capacity(), remaining));
                int read = channel.read(fallback);
                if (read < 0) {
                    throw new IOException("Server closed before sending the requested range");
                }
                if (read == 0) {
                    continue;
                }
                fallback.flip();
                while (fallback.hasRemaining()) {
                    int written = output.write(fallback, position);
                    if (written == 0) {
                        continue;
                    }
                    position += written;
                    remaining -= written;
                }
            }
        }
    }

    private static void mergeParts(List<Path> parts, Path output, long expectedSize, TransferProtocol.Mode mode)
            throws IOException {
        long position = 0;
        if (mode == TransferProtocol.Mode.nio) {
            try (FileChannel destination = FileChannel.open(output, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer fallback = ByteBuffer.allocateDirect(64 * 1024);
                for (Path part : parts) {
                    try (FileChannel source = FileChannel.open(part, StandardOpenOption.READ)) {
                        long remaining = source.size();
                        long sourcePosition = 0;
                        while (remaining > 0) {
                            destination.position(position);
                            long copied = source.transferTo(sourcePosition, remaining, destination);
                            if (copied > 0) {
                                sourcePosition += copied;
                                position += copied;
                                remaining -= copied;
                                continue;
                            }
                            fallback.clear();
                            fallback.limit((int) Math.min(fallback.capacity(), remaining));
                            int read = source.read(fallback, sourcePosition);
                            if (read < 0) {
                                throw new IOException("Part file ended during merge");
                            }
                            if (read == 0) {
                                continue;
                            }
                            fallback.flip();
                            while (fallback.hasRemaining()) {
                                int written = destination.write(fallback, position);
                                if (written == 0) {
                                    continue;
                                }
                                position += written;
                                sourcePosition += written;
                                remaining -= written;
                            }
                        }
                    }
                }
            }
        } else {
            try (OutputStream destination = Files.newOutputStream(output, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                byte[] buffer = new byte[64 * 1024];
                for (Path part : parts) {
                    try (InputStream source = Files.newInputStream(part)) {
                        int count;
                        while ((count = source.read(buffer)) >= 0) {
                            destination.write(buffer, 0, count);
                            position += count;
                        }
                    }
                }
            }
        }
        if (position != expectedSize) {
            throw new IOException("Merged size mismatch: expected " + expectedSize + ", got " + position);
        }
    }

    private static long getSize(String host, int port, String fileName) throws IOException {
        String response = request(host, port, "INFO " + TransferProtocol.encodeName(fileName));
        String[] fields = response.split(" ");
        if (fields.length != 2 || !fields[0].equals("SIZE")) {
            throw new IOException("Unexpected INFO response: " + response);
        }
        try {
            long size = Long.parseLong(fields[1]);
            if (size < 0) {
                throw new IOException("Server returned a negative file size");
            }
            return size;
        } catch (NumberFormatException e) {
            throw new IOException("Invalid file size in server response", e);
        }
    }

    private static String getHash(String host, int port, String fileName) throws IOException {
        String response = request(host, port, "HASH " + TransferProtocol.encodeName(fileName));
        String[] fields = response.split(" ");
        if (fields.length != 2 || !fields[0].equals("SHA256") || !fields[1].matches("[0-9a-f]{64}")) {
            throw new IOException("Unexpected HASH response: " + response);
        }
        return fields[1];
    }

    private static String request(String host, int port, String command) throws IOException {
        try (SocketChannel channel = TransferProtocol.connect(host, port)) {
            TransferProtocol.writeLine(channel, command);
            return TransferProtocol.requireOk(TransferProtocol.readLine(channel));
        }
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
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

    private static void deleteTree(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }
        String command = args[0];
        String host = "127.0.0.1";
        int port = TransferProtocol.DEFAULT_PORT;
        int workers = TransferProtocol.DEFAULT_WORKERS;
        TransferProtocol.Mode mode = TransferProtocol.Mode.nio;
        String file = null;
        Path output = null;
        Path logPath = TransferRunLog.defaultLogPath();
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = requireValue(args, ++i, "--host");
                case "--port" -> port = Integer.parseInt(requireValue(args, ++i, "--port"));
                case "--workers" -> workers = Integer.parseInt(requireValue(args, ++i, "--workers"));
                case "--mode" -> mode = TransferProtocol.Mode.parse(requireValue(args, ++i, "--mode"));
                case "--file" -> file = requireValue(args, ++i, "--file");
                case "--out" -> output = Path.of(requireValue(args, ++i, "--out"));
                case "--log" -> logPath = Path.of(requireValue(args, ++i, "--log"));
                default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
            }
        }
        switch (command) {
            case "list" -> list(host, port);
            case "info" -> {
                requireFile(file);
                System.out.println("SIZE " + getSize(host, port, file));
            }
            case "download" -> {
                requireFile(file);
                if (output == null) {
                    output = Path.of(file).getFileName();
                }
                DownloadResult result = download(host, port, file, output, workers, mode, logPath);
                System.out.printf("Downloaded %d bytes to %s%nSHA-256: %s%nMode: %s; workers: %d; parts: %d%n",
                        result.size(), output.toAbsolutePath(), result.sha256(), mode, workers,
                        TransferProtocol.PARTS);
                System.out.printf("Elapsed: %.6f s; throughput: %.3f MB/s%n",
                        result.seconds(), result.megabytesPerSecond());
                System.out.println("Run metrics appended to " + logPath.toAbsolutePath().normalize());
            }
            default -> throw new IllegalArgumentException("Command must be list, info, or download");
        }
    }

    private static void list(String host, int port) throws IOException {
        try (SocketChannel channel = TransferProtocol.connect(host, port)) {
            TransferProtocol.writeLine(channel, "LIST");
            while (true) {
                String line = TransferProtocol.readLine(channel);
                if (line.equals("END")) {
                    return;
                }
                TransferProtocol.requireOk(line);
                String[] fields = line.split(" ");
                if (fields.length != 3 || !fields[0].equals("FILE")) {
                    throw new IOException("Unexpected LIST response: " + line);
                }
                System.out.println(TransferProtocol.decodeName(fields[1]) + "\t" + fields[2]);
            }
        }
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + option);
        }
        return args[index];
    }

    private static void requireFile(String file) {
        if (file == null || file.isBlank()) {
            throw new IllegalArgumentException("Use --file to specify the remote filename");
        }
    }

    private static void printUsage() {
        System.out.println("""
                Usage:
                  java FileTransferClient list [--host HOST] [--port PORT]
                  java FileTransferClient info --file NAME [--host HOST] [--port PORT]
                  java FileTransferClient download --file NAME --out PATH [--host HOST] [--port PORT]
                      [--workers COUNT] [--mode traditional|nio] [--log CSV_PATH]
                """);
    }
}
