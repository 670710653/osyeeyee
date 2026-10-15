import java.util.concurrent.BlockingQueue;

public class Scheduler extends Thread {

    private final BlockingQueue<Job> arrivalQueue;
    private final ReadyQueue readyQueue;
    private final ProjectLogger logger;
    private volatile boolean running = true;

    public Scheduler(BlockingQueue<Job> arrivalQueue, ReadyQueue readyQueue, ProjectLogger logger) {
        super("scheduler");
        this.arrivalQueue = arrivalQueue;
        this.readyQueue = readyQueue;
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
                Job job = arrivalQueue.take();
                if (job == JobGenerator.END_SIGNAL) {
                    readyQueue.close();
                    logger.systemEvent("scheduler closed ready queue after all jobs arrived");
                    return;
                }
                readyQueue.add(job);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
