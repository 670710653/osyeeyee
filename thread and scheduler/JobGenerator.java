import java.util.List;
import java.util.concurrent.BlockingQueue;

public class JobGenerator extends Thread {

    public static final Job END_SIGNAL =
            new Job("__END__", -1L, 1, 0, ResourceType.NONE, 0, Integer.MAX_VALUE);

    private final List<Job> jobs;
    private final BlockingQueue<Job> arrivalQueue;
    private final ProjectLogger logger;
    private final long simulationStartNanos;

    public JobGenerator(List<Job> jobs, BlockingQueue<Job> arrivalQueue, ProjectLogger logger) {
        super("generator");
        this.jobs = jobs;
        this.arrivalQueue = arrivalQueue;
        this.logger = logger;
        this.simulationStartNanos = System.nanoTime();
    }

    @Override
    public void run() {
        for (Job job : jobs) {
            long nowMs = (System.nanoTime() - simulationStartNanos) / 1_000_000L;
            long waitMs = job.arrivalMs - nowMs;
            if (waitMs > 0L) {
                try {
                    Thread.sleep(waitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            long actualArrival = logger.now();
            job.markActualArrival(actualArrival);
            logger.jobArrived(job);
            try {
                arrivalQueue.put(job);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        try {
            arrivalQueue.put(END_SIGNAL);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
