import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public final class FileTransferServer implements AutoCloseable {
    private final Path root;
    private final int requestedPort;
    private final ExecutorService handlers;
    private ServerSocketChannel listener;

    public FileTransferServer(Path root, int port, int handlerThreads) throws IOException {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535");
        }
        if (handlerThreads < 1) {
            throw new IllegalArgumentException("Server threads must be at least 1");
        }
        Files.createDirectories(root);
        this.root = root.toRealPath();
        this.requestedPort = port;
        this.handlers = Executors.newFixedThreadPool(handlerThreads);
    }

    public int start() throws IOException {
        listener = ServerSocketChannel.open();
        listener.bind(new java.net.InetSocketAddress(requestedPort));
        return ((java.net.InetSocketAddress) listener.getLocalAddress()).getPort();
    }

    public void serve() throws IOException {
        if (listener == null) {
            start();
        }
        while (listener.isOpen()) {
            try {
                SocketChannel client = listener.accept();
                handlers.execute(() -> handle(client));
            } catch (IOException e) {
                if (listener.isOpen()) {
                    throw e;
                }
            }
        }
    }

    private void handle(SocketChannel client) {
        try (client) {
            try {
                String request = TransferProtocol.readLine(client);
                String[] fields = request.split(" ", -1);
                switch (fields[0]) {
                    case "LIST" -> list(client, fields);
                    case "INFO" -> info(client, fields);
                    case "HASH" -> hash(client, fields);
                    case "GET" -> get(client, fields);
                    default -> error(client, "BAD_REQUEST", "Unknown command");
                }
            } catch (Exception e) {
                System.err.println("Request failed: " + e.getMessage());
                try {
                    error(client, "BAD_REQUEST", safeMessage(e.getMessage()));
                } catch (IOException writeFailure) {
                    System.err.println("Could not send request error: " + writeFailure.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Connection close failed: " + e.getMessage());
        }
    }

    private void list(SocketChannel client, String[] fields) throws IOException {
        if (fields.length != 1) {
            error(client, "BAD_REQUEST", "LIST takes no arguments");
            return;
        }
        try (Stream<Path> paths = Files.list(root)) {
            List<Path> files = paths.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .sorted()
                    .toList();
            for (Path file : files) {
                TransferProtocol.writeLine(client, "FILE "
                        + TransferProtocol.encodeName(file.getFileName().toString()) + " "
                        + Files.size(file));
            }
        }
        TransferProtocol.writeLine(client, "END");
    }

    private void info(SocketChannel client, String[] fields) throws IOException {
        if (fields.length != 2) {
            error(client, "BAD_REQUEST", "INFO requires one filename");
            return;
        }
        Path file = resolveFile(fields[1]);
        TransferProtocol.writeLine(client, "SIZE " + Files.size(file));
    }

    private void hash(SocketChannel client, String[] fields) throws IOException {
        if (fields.length != 2) {
            error(client, "BAD_REQUEST", "HASH requires one filename");
            return;
        }
        TransferProtocol.writeLine(client, "SHA256 " + sha256(resolveFile(fields[1])));
    }

    private void get(SocketChannel client, String[] fields) throws IOException {
        if (fields.length != 5) {
            error(client, "BAD_REQUEST", "GET requires filename, offset, length, and mode");
            return;
        }
        Path file = resolveFile(fields[1]);
        long offset;
        long length;
        TransferProtocol.Mode mode;
        try {
            offset = Long.parseLong(fields[2]);
            length = Long.parseLong(fields[3]);
            mode = TransferProtocol.Mode.parse(fields[4]);
        } catch (IllegalArgumentException e) {
            error(client, "BAD_REQUEST", e.getMessage());
            return;
        }
        long size = Files.size(file);
        if (offset < 0 || length < 0 || offset > size || length > size - offset) {
            error(client, "INVALID_RANGE", "Requested byte range is outside the file");
            return;
        }

        TransferProtocol.writeLine(client, "OK " + length);
        if (length == 0) {
            return;
        }
        if (mode == TransferProtocol.Mode.nio) {
            sendWithTransferTo(client, file, offset, length);
        } else {
            sendWithTraditionalIo(client, file, offset, length);
        }
    }

    private Path resolveFile(String encodedName) throws IOException {
        String name;
        try {
            name = TransferProtocol.decodeName(encodedName);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid filename encoding");
        }
        Path candidate = root.resolve(name).normalize();
        if (!candidate.startsWith(root) || candidate.equals(root)) {
            throw new IOException("Filename is outside the shared directory");
        }
        Path real = candidate.toRealPath();
        if (!real.startsWith(root) || !Files.isRegularFile(real)) {
            throw new IOException("File does not exist in the shared directory");
        }
        return real;
    }

    private static void sendWithTraditionalIo(SocketChannel client, Path file, long offset, long length)
            throws IOException {
        try (InputStream input = Files.newInputStream(file);
             OutputStream output = client.socket().getOutputStream()) {
            input.skipNBytes(offset);
            byte[] buffer = new byte[64 * 1024];
            long remaining = length;
            while (remaining > 0) {
                int count = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (count < 0) {
                    throw new IOException("Shared file ended before the requested range");
                }
                output.write(buffer, 0, count);
                remaining -= count;
            }
            output.flush();
        }
    }

    private static void sendWithTransferTo(SocketChannel client, Path file, long offset, long length)
            throws IOException {
        try (FileChannel input = FileChannel.open(file, StandardOpenOption.READ)) {
            long position = offset;
            long remaining = length;
            ByteBuffer fallback = ByteBuffer.allocateDirect(64 * 1024);
            while (remaining > 0) {
                long sent = input.transferTo(position, remaining, client);
                if (sent > 0) {
                    position += sent;
                    remaining -= sent;
                    continue;
                }
                fallback.clear();
                fallback.limit((int) Math.min(fallback.capacity(), remaining));
                int read = input.read(fallback, position);
                if (read < 0) {
                    throw new IOException("Shared file ended before the requested range");
                }
                if (read == 0) {
                    continue;
                }
                fallback.flip();
                while (fallback.hasRemaining()) {
                    client.write(fallback);
                }
                position += read;
                remaining -= read;
            }
        }
    }

    private static String sha256(Path file) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(file)) {
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

    private static void error(SocketChannel client, String code, String message) throws IOException {
        TransferProtocol.writeLine(client, "ERROR " + code + " " + safeMessage(message));
    }

    private static String safeMessage(String message) {
        if (message == null || message.isBlank()) {
            return "Request failed";
        }
        return message.replace('\n', ' ').replace('\r', ' ');
    }

    @Override
    public void close() throws IOException {
        if (listener != null) {
            listener.close();
        }
        handlers.shutdownNow();
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of("shared");
        int port = TransferProtocol.DEFAULT_PORT;
        int threads = Math.max(32, Runtime.getRuntime().availableProcessors() * 2);
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--root" -> root = Path.of(requireValue(args, ++i, "--root"));
                case "--port" -> port = Integer.parseInt(requireValue(args, ++i, "--port"));
                case "--threads" -> threads = Integer.parseInt(requireValue(args, ++i, "--threads"));
                default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
            }
        }
        FileTransferServer server = new FileTransferServer(root, port, threads);
        int boundPort = server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (IOException e) {
                System.err.println("Server shutdown failed: " + e.getMessage());
            }
        }));
        System.out.println("Listening on port " + boundPort + ", sharing " + server.root
                + ", handler threads " + threads);
        server.serve();
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + option);
        }
        return args[index];
    }
}
