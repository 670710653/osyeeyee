import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Multi-threaded File Download Server (Zero-copy)
 *
 * Protocol (text header ลงท้ายด้วย '\n', payload เป็น binary):
 *   LIST                          -> FILE <name> <size>\n ... END\n
 *   INFO <filename>               -> SIZE <bytes>\n   | ERROR <code> <message>\n
 *   HASH <filename>               -> HASH <sha256-hex>\n | ERROR ...
 *   GET <filename> <offset> <len> -> OK <len>\n + payload (<len> bytes) | ERROR ...
 *   QUIT                          -> BYE\n
 *
 * Error code: 400 = request ผิดรูปแบบ, 404 = ไม่พบไฟล์, 416 = ช่วงข้อมูลไม่ถูกต้อง
 *
 * Zero-copy: ใช้ FileChannel.transferTo(position, count, socketChannel)
 *            ซึ่งบน Linux/macOS จะใช้ sendfile() ส่งข้อมูลจาก page cache ไป socket ใน kernel โดยตรง
 *
 * Concurrency: Thread Pool (platform thread) 1 connection ต่อ 1 thread
 *   ไม่ใช้ Virtual Thread เพราะ transferTo() บน virtual thread จะคืนค่า 0 เมื่อ send buffer ของ socket เต็ม
 *   (ไม่รอให้เขียนได้) ทำให้ส่งไฟล์ใหญ่ไม่ครบ ส่วน platform thread จะ block รอจนส่งได้ตามปกติ
 *
 * Compile: javac -encoding UTF-8 ZeroCopyServer.java
 * Run    : java ZeroCopyServer [port=9000] [rootDir=./files]
 */
public class ZeroCopyServer {

    private static final int DEFAULT_PORT = 9000;
    private static final int MAX_LINE = 4096;

    private final Path root;
    private final Map<String, String> hashCache = new ConcurrentHashMap<>();

    public ZeroCopyServer(Path root) {
        this.root = root.toAbsolutePath().normalize();
    }

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Path dir = Paths.get(args.length > 1 ? args[1] : "files");
        Files.createDirectories(dir);
        new ZeroCopyServer(dir).start(port);
    }

    public void start(int port) throws IOException {
        try (ServerSocketChannel server = ServerSocketChannel.open();
             ExecutorService pool = Executors.newCachedThreadPool()) {
            server.bind(new InetSocketAddress(port), 128);
            log("Server listening on port " + port + " | root = " + root);
            while (true) {
                SocketChannel client = server.accept();   // blocking accept
                pool.execute(() -> handle(client));        // 1 connection = 1 thread
            }
        }
    }

    // ------------------------------------------------------------------
    // Connection handler
    // ------------------------------------------------------------------
    private void handle(SocketChannel ch) {
        String peer = remote(ch);
        try (ch) {
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            String line;
            while ((line = readLine(ch)) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] p = line.split("\\s+");
                String cmd = p[0].toUpperCase(Locale.ROOT);
                switch (cmd) {
                    case "LIST" -> doList(ch);
                    case "INFO" -> doInfo(ch, p);
                    case "HASH" -> doHash(ch, p);
                    case "GET"  -> doGet(ch, p, peer);
                    case "QUIT" -> {
                        writeLine(ch, "BYE");
                        return;
                    }
                    default -> writeLine(ch, "ERROR 400 Unknown command " + cmd);
                }
            }
        } catch (IOException e) {
            log(peer + " connection closed with error: " + e.getMessage());
        }
    }

    private void doList(SocketChannel ch) throws IOException {
        StringBuilder sb = new StringBuilder();
        List<Path> files;
        try (Stream<Path> s = Files.list(root)) {
            files = s.filter(Files::isRegularFile).sorted().toList();
        }
        for (Path p : files) {
            String name = p.getFileName().toString();
            if (name.chars().anyMatch(Character::isWhitespace)) continue; // protocol แยกด้วยช่องว่าง จึงข้ามชื่อที่มี space
            sb.append("FILE ").append(name).append(' ').append(Files.size(p)).append('\n');
        }
        writeLine(ch, sb + "END");
    }

    private void doInfo(SocketChannel ch, String[] p) throws IOException {
        if (p.length != 2) {
            writeLine(ch, "ERROR 400 Usage: INFO <filename>");
            return;
        }
        Path f = lookup(ch, p[1]);
        if (f == null) return;
        writeLine(ch, "SIZE " + Files.size(f));
    }

    private void doHash(SocketChannel ch, String[] p) throws IOException {
        if (p.length != 2) {
            writeLine(ch, "ERROR 400 Usage: HASH <filename>");
            return;
        }
        Path f = lookup(ch, p[1]);
        if (f == null) return;
        String key = f + "|" + Files.size(f) + "|" + Files.getLastModifiedTime(f).toMillis();
        String hash = hashCache.get(key);
        if (hash == null) {
            hash = sha256(f);
            hashCache.put(key, hash);
        }
        writeLine(ch, "HASH " + hash);
    }

    /** GET <filename> <offset> <length> : ส่งเฉพาะช่วง [offset, offset+length) ด้วย zero-copy */
    private void doGet(SocketChannel ch, String[] p, String peer) throws IOException {
        if (p.length != 4) {
            writeLine(ch, "ERROR 400 Usage: GET <filename> <offset> <length>");
            return;
        }
        long offset, length;
        try {
            offset = Long.parseLong(p[2]);
            length = Long.parseLong(p[3]);
        } catch (NumberFormatException e) {
            writeLine(ch, "ERROR 400 offset/length must be integers");
            return;
        }
        Path f = lookup(ch, p[1]);
        if (f == null) return;

        try (FileChannel fc = FileChannel.open(f, StandardOpenOption.READ)) {
            long size = fc.size();
            if (offset < 0 || length < 0 || offset > size || length > size - offset) {
                writeLine(ch, "ERROR 416 Range not satisfiable (file size = " + size + ")");
                return;
            }

            writeLine(ch, "OK " + length);   // header ก่อน แล้วตามด้วย payload

            long pos = offset;
            long remaining = length;
            while (remaining > 0) {
                // transferTo อาจส่งได้น้อยกว่า count จึงต้องวนซ้ำ
                long n = fc.transferTo(pos, remaining, ch);
                if (n <= 0) {
                    throw new IOException("transferTo made no progress at position " + pos);
                }
                pos += n;
                remaining -= n;
            }
        }
        log(peer + " GET " + p[1] + " offset=" + offset + " length=" + length + " done");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** ตรวจชื่อไฟล์ + หาไฟล์ ถ้าไม่ผ่านจะส่ง ERROR ให้ client แล้ว return null */
    private Path lookup(SocketChannel ch, String name) throws IOException {
        if (name.contains("/") || name.contains("\\") || name.equals(".") || name.equals("..")) {
            writeLine(ch, "ERROR 400 Invalid file name");
            return null;
        }
        try {
            Path f = root.resolve(name).normalize();
            if (f.startsWith(root) && Files.isRegularFile(f)) return f;
        } catch (InvalidPathException ignored) {
            // ตกไปที่ 404
        }
        writeLine(ch, "ERROR 404 File not found: " + name);
        return null;
    }

    static String sha256(Path file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
                ByteBuffer buf = ByteBuffer.allocateDirect(1 << 20);
                while (fc.read(buf) != -1) {
                    buf.flip();
                    md.update(buf);
                    buf.clear();
                }
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * อ่านทีละ 1 byte จนเจอ '\n' โดยไม่ใช้ BufferedReader
     * เพื่อไม่ให้ buffer ไปกิน payload ที่ตามหลัง header
     */
    static String readLine(SocketChannel ch) throws IOException {
        ByteBuffer one = ByteBuffer.allocate(1);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(64);
        while (true) {
            one.clear();
            int n = ch.read(one);
            if (n < 0) return null;            // connection ปิดแล้ว
            if (n == 0) continue;
            byte b = one.get(0);
            if (b == '\n') break;
            if (b != '\r') bos.write(b);
            if (bos.size() > MAX_LINE) throw new IOException("Line too long");
        }
        return bos.toString(StandardCharsets.UTF_8);
    }

    static void writeLine(SocketChannel ch, String s) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap((s + "\n").getBytes(StandardCharsets.UTF_8));
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    private static String remote(SocketChannel ch) {
        try {
            return String.valueOf(ch.getRemoteAddress());
        } catch (IOException e) {
            return "?";
        }
    }

    private static void log(String msg) {
        System.out.println(LocalTime.now().withNano(0) + "  " + msg);
    }
}
