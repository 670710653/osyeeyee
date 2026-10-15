/**
 * Thread ที่รายงานสถานะระบบเป็นระยะ
 */
public class Monitor extends Thread {

    private final ReadyQueue readyQueue;
    private final ResourceManager resources;
    private final Statistics statistics;
    private final ProjectLogger logger;
    private volatile boolean running = true;

    public Monitor(ReadyQueue readyQueue, ResourceManager resources,
                   Statistics statistics, ProjectLogger logger) {
        super("monitor");
        this.readyQueue = readyQueue;
        this.resources = resources;
        this.statistics = statistics;
        this.logger = logger;
    }

    public void shutdown() {
        this.running = false;
        interrupt();
    }

    @Override
    public void run() {
        while (running && !isInterrupted()) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            logger.monitor(readyQueue.size(), statistics.runningCount(), statistics.completedCount(), resources.status());
        }
    }
}
