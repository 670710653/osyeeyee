import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Main {

    public static void main(String[] args) {
        try {
            System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8));
        } catch (Exception ignored) {
            // Ignore unsupported console encoding; the app still functions.
        }

        Config config;
        try {
            config = Config.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("ผิดพลาด: " + e.getMessage());
            System.err.println();
            System.err.println(Config.USAGE);
            System.exit(1);
            return;
        }

        ProjectLogger logger = new ProjectLogger();
        List<Job> jobs;
        try {
            jobs = WorkloadLoader.load(config.workloadPath);
        } catch (WorkloadFormatException e) {
            System.err.println("ไฟล์ workload ผิดรูปแบบ — " + e.getMessage());
            System.exit(1);
            return;
        } catch (java.io.IOException e) {
            System.err.println("เปิดไฟล์ \"" + config.workloadPath + "\" ไม่ได้");
            System.err.println("ตรวจว่าไฟล์มีอยู่จริงและ path ถูกต้อง (สั่ง java จากโฟลเดอร์ใด)");
            System.exit(1);
            return;
        }

        logger.systemStart(config);
        logger.systemEvent("โหลดงานได้ " + jobs.size() + " ชิ้น");

        ResourceManager resources = new ResourceManager(config.printerPermits, config.databasePermits);
        ReadyQueue readyQueue = new ReadyQueue(config.policy);
        Statistics statistics = new Statistics();
        BlockingQueue<Job> arrivalQueue = new ArrayBlockingQueue<>(jobs.size() + 1);

        List<Worker> workers = new ArrayList<>();
        for (int i = 1; i <= config.workers; i++) {
            workers.add(new Worker("worker-" + i, readyQueue, resources, statistics, logger));
        }

        Monitor monitor = new Monitor(readyQueue, resources, statistics, logger);
        JobGenerator generator = new JobGenerator(jobs, arrivalQueue, logger);
        Scheduler scheduler = new Scheduler(arrivalQueue, readyQueue, logger);

        for (Worker worker : workers) {
            worker.start();
        }
        monitor.start();
        scheduler.start();
        generator.start();

        while (statistics.completedCount() < jobs.size()) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        readyQueue.close();
        scheduler.shutdown();
        monitor.shutdown();
        generator.interrupt();

        for (Worker worker : workers) {
            worker.interrupt();
        }

        for (Worker worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        try {
            scheduler.join();
            monitor.join();
            generator.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long makespanMs = logger.now();
        statistics.printSummary(jobs, makespanMs);
        logger.systemStop(statistics.completedCount(), jobs.size());
    }
}
