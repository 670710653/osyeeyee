import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class TransferProtocol {
    static final int DEFAULT_PORT = 5000;
    static final int PARTS = 10;
    static final int DEFAULT_WORKERS = 20;
    static final int MAX_LINE_BYTES = 16 * 1024;

    enum Mode {
        traditional,
        nio;

        static Mode parse(String value) {
            for (Mode mode : values()) {
                if (mode.name().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Mode must be traditional or nio");
        }
    }

    private TransferProtocol() {
    }

    static String encodeName(String name) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(name.getBytes(StandardCharsets.UTF_8));
    }

    static String decodeName(String encoded) {
        return new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    static SocketChannel connect(String host, int port) throws IOException {
        SocketChannel channel = SocketChannel.open();
        try {
            channel.configureBlocking(true);
            channel.socket().connect(new InetSocketAddress(host, port), 10_000);
            return channel;
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    static void writeLine(SocketChannel channel, String line) throws IOException {
        ByteBuffer bytes = StandardCharsets.UTF_8.encode(line + "\n");
        while (bytes.hasRemaining()) {
            channel.write(bytes);
        }
    }

    static String readLine(SocketChannel channel) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ByteBuffer oneByte = ByteBuffer.allocate(1);
        while (bytes.size() <= MAX_LINE_BYTES) {
            oneByte.clear();
            int count = channel.read(oneByte);
            if (count < 0) {
                if (bytes.size() == 0) {
                    throw new IOException("Connection closed before response line");
                }
                throw new IOException("Connection closed in the middle of a response line");
            }
            if (count == 0) {
                continue;
            }
            byte value = oneByte.array()[0];
            if (value == '\n') {
                byte[] line = bytes.toByteArray();
                int length = line.length;
                if (length > 0 && line[length - 1] == '\r') {
                    length--;
                }
                return new String(line, 0, length, StandardCharsets.UTF_8);
            }
            bytes.write(value);
        }
        throw new IOException("Protocol line exceeds " + MAX_LINE_BYTES + " bytes");
    }

    static String requireOk(String response) throws IOException {
        if (response.startsWith("ERROR ")) {
            throw new IOException("Server: " + response);
        }
        return response;
    }
}
