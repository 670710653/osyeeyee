import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.HexFormat;

/**
 * Multi-threaded File Download Client (Zero-copy)
 *
 * - แบ่งไฟล์เป็น N segments (default 10) ไม่ซ้อนกัน, segment สุดท้ายรับ byte ที่เหลือ
 * - ใช้ thread pool ขนาด M workers (default 20) ดึง segment ไปโหลด
 *   แต่ละ segment = 1 connection + offset + length ของตัวเอง
 * - เขียนลง final file ตาม offset โดยตรง ด้วย FileChannel.transferFrom(socketChannel, position, count)
 *   (แต่ละ task เปิด FileChannel ของตัวเอง และ pre-allocate ไฟล์ด้วย setLength ก่อน)
 * - ตรวจ size + SHA-256 เทียบกับที่ server คำนวณ แล้ววัดเวลา/throughput (MB/s)
 *
 * หมายเหตุ: ถ้า workers > segments จะมี worker ว่าง (มีงานแค่ segments ชิ้น)
 *           ถ้าต้องการให้ 20 workers ทำงานพร้อมกันทั้งหมด ให้ตั้ง segments >= 20
 *
 * Compile: javac -encoding UTF-8 ZeroCopyClient.java
 * Run    : java ZeroCopyClient <host> <port> list
 *          java ZeroCopyClient <host> <port> get <filename> [segments=10] [workers=20] [runs=1]
 * Output : ./downloads/<filename>
 */
public class ZeroCopyClient {

    private static final int MAX_LINE = 4096;
    private static final Path OUT_DIR = Paths.get("downloads");

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            usage();
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String cmd = args[2].toLowerCase(Locale.ROOT);

        switch (cmd) {
            case "list" -> list(host, port);
            case "get" -> {
                if (args.length < 4) {
                    usage();
                    return;
                }
                String file = args[3];
                int segments = args.length > 4 ? Integer.parseInt(args[4]) : 10;
                int workers  = args.length > 5 ? Integer.parseInt(args[5]) : 20;
                int runs     = args.length > 6 ? Integer.parseInt(args[6]) : 1;
                if (segments < 1 || workers < 1 || runs < 1) {
                    System.err.println("segments, workers, runs must be >= 1");
                    System.exit(2);
                }
                boolean ok = benchmark(host, port, file, segments, workers, runs);
                System.exit(ok ? 0 : 1);
            }
            default -> usage();
        }
    }

    private static void usage() {
        System.out.println("""
                Usage:
                  java ZeroCopyClient <host> <port> list
                  java ZeroCopyClient <host> <port> get <filename> [segments=10] [workers=20] [runs=1]""");
    }

    // ------------------------------------------------------------------
    // LIST
    // ------------------------------------------------------------------
    private static void list(String host, int port) throws IOException {
        try (SocketChannel ch = connect(host, port)) {
            writeLine(ch, "LIST");
            String line;
            while ((line = readLine(ch)) != null && !line.equals("END")) {
                System.out.println(line);
            }
            writeLine(ch, "QUIT");
        }
    }

    // ------------------------------------------------------------------
    // INFO + HASH + download (หลายรอบ) + verify
    // ------------------------------------------------------------------
    private static boolean benchmark(String host, int port, String file,
                                     int segments, int workers, int runs) throws Exception {
        long size;
        String expectedHash;
        try (SocketChannel ch = connect(host, port)) {
            size = Long.parseLong(field(request(ch, "INFO " + file), "SIZE"));
            System.out.println("Waiting for server to compute SHA-256 ...");
            expectedHash = field(request(ch, "HASH " + file), "HASH");
            writeLine(ch, "QUIT");
        }

        int effectiveSegments = (int) Math.max(1, Math.min(segments, size));
        System.out.printf("File: %s | size = %,d bytes (%.2f MB)%n", file, size, size / MB);
        System.out.printf("Segments = %d | Workers = %d | Runs = %d%n", effectiveSegments, workers, runs);
        if (workers > effectiveSegments) {
            System.out.printf("WARNING: workers (%d) > segments (%d) -> only %d workers will have work%n",
                    workers, effectiveSegments, effectiveSegments);
        }

        Files.createDirectories(OUT_DIR);
        Path out = OUT_DIR.resolve(file);

        double sumSec = 0, sumMbps = 0, minSec = Double.MAX_VALUE, maxSec = 0;
        boolean allOk = true;

        for (int run = 1; run <= runs; run++) {
            Files.deleteIfExists(out);
            System.out.printf("%n--- Run %d/%d ---%n", run, runs);

            long t0 = System.nanoTime();
            download(host, port, file, size, effectiveSegments, workers, out, runs == 1);
            double sec = (System.nanoTime() - t0) / 1e9;
            double mbps = (size / MB) / sec;

            boolean sizeOk = Files.size(out) == size;
            boolean hashOk = expectedHash.equalsIgnoreCase(sha256(out));
            allOk &= sizeOk && hashOk;

            System.out.printf("Run %d: time = %.3f s | throughput = %.2f MB/s | size %s | hash %s%n",
                    run, sec, mbps, sizeOk ? "OK" : "MISMATCH", hashOk ? "OK" : "MISMATCH");

            sumSec += sec;
            sumMbps += mbps;
            minSec = Math.min(minSec, sec);
            maxSec = Math.max(maxSec, sec);
        }

        System.out.printf("%n=== Summary (%d runs, %d segments, %d workers) ===%n", runs, effectiveSegments, workers);
        System.out.printf("avg time = %.3f s (min %.3f, max %.3f) | avg throughput = %.2f MB/s | verify = %s%n",
                sumSec / runs, minSec, maxSec, sumMbps / runs, allOk ? "ALL OK" : "FAILED");
        return allOk;
    }

    private static final double MB = 1024.0 * 1024.0;

    // ------------------------------------------------------------------
    // Download: แบ่ง segment -> thread pool -> แต่ละ task = 1 connection
    // ------------------------------------------------------------------
    private static void download(String host, int port, String file, long size,
                                 int segments, int workers, Path out, boolean verbose) throws Exception {
        // pre-allocate final file: transferFrom จะไม่เขียนถ้า position เกินขนาดไฟล์ปัจจุบัน
        try (RandomAccessFile raf = new RandomAccessFile(out.toFile(), "rw")) {
            raf.setLength(size);
        }
        if (size == 0) return;

        long[][] ranges = splitRanges(size, segments);
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<Long>> futures = new ArrayList<>();
            for (int i = 0; i < ranges.length; i++) {
                final int id = i;
                final long offset = ranges[i][0];
                final long length = ranges[i][1];
                Callable<Long> task = () -> {
                    long t0 = System.nanoTime();
                    long got = downloadRange(host, port, file, out, offset, length);
                    if (verbose) {
                        System.out.printf("  segment %2d  offset=%,d  length=%,d  %.3f s  [%s]%n",
                                id, offset, length, (System.nanoTime() - t0) / 1e9,
                                Thread.currentThread().getName());
                    }
                    return got;
                };
                futures.add(pool.submit(task));
            }

            long total = 0;
            for (Future<Long> f : futures) {
                try {
                    total += f.get();
                } catch (ExecutionException e) {
                    throw new IOException("worker failed: " + e.getCause(), e.getCause());
                }
            }
            if (total != size) {
                throw new IOException("received " + total + " bytes, expected " + size);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /** แบ่งไฟล์เป็น n ช่วงไม่ซ้อนกัน; ช่วงสุดท้ายรับ byte ที่เหลือ */
    static long[][] splitRanges(long size, int segments) {
        long base = size / segments;
        long[][] r = new long[segments][2];
        for (int i = 0; i < segments; i++) {
            long off = i * base;
            r[i][0] = off;
            r[i][1] = (i == segments - 1) ? size - off : base;
        }
        return r;
    }

    /** 1 task: เปิด connection + FileChannel ของตัวเอง ส่ง GET แล้วรับข้อมูลด้วย transferFrom */
    private static long downloadRange(String host, int port, String file, Path out,
                                      long offset, long length) throws IOException {
        try (SocketChannel sc = connect(host, port);
             FileChannel fc = FileChannel.open(out, StandardOpenOption.WRITE)) {

            writeLine(sc, "GET " + file + " " + offset + " " + length);

            String header = readLine(sc);
            if (header == null) throw new EOFException("server closed connection before response");
            if (header.startsWith("ERROR")) throw new IOException("server " + header);
            long expected = Long.parseLong(field(header, "OK"));
            if (expected != length) {
                throw new IOException("server will send " + expected + " bytes, requested " + length);
            }

            long received = 0;
            while (received < length) {
                // รับจาก socket เขียนลง final file ที่ตำแหน่ง offset + received
                long n = fc.transferFrom(sc, offset + received, length - received);
                if (n <= 0) {
                    throw new EOFException("connection closed early: " + received + "/" + length + " bytes");
                }
                received += n;
            }
            return received;
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private static SocketChannel connect(String host, int port) throws IOException {
        SocketChannel ch = SocketChannel.open(new InetSocketAddress(host, port));
        ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
        return ch;
    }

    /** ส่งคำสั่งแล้วรอ response 1 บรรทัด; ถ้าเป็น ERROR จะ throw */
    private static String request(SocketChannel ch, String cmd) throws IOException {
        writeLine(ch, cmd);
        String resp = readLine(ch);
        if (resp == null) throw new EOFException("server closed connection");
        if (resp.startsWith("ERROR")) throw new IOException("server " + resp);
        return resp;
    }

    /** ดึงค่าหลัง keyword เช่น field("SIZE 123", "SIZE") -> "123" */
    private static String field(String line, String keyword) throws IOException {
        String[] p = line.trim().split("\\s+");
        if (p.length < 2 || !p[0].equals(keyword)) {
            throw new IOException("unexpected response: " + line);
        }
        return p[1];
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

    /** อ่านทีละ 1 byte จนเจอ '\n' เพื่อไม่ให้ไปอ่านกิน payload ที่ตามหลัง header */
    static String readLine(SocketChannel ch) throws IOException {
        ByteBuffer one = ByteBuffer.allocate(1);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(64);
        while (true) {
            one.clear();
            int n = ch.read(one);
            if (n < 0) return null;
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
}
