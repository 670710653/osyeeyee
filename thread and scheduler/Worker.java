public class Worker extends Thread {

    private final ReadyQueue readyQueue;
    private final ResourceManager resources;
    private final Statistics statistics;
    private final ProjectLogger logger;

    public Worker(String name, ReadyQueue readyQueue, ResourceManager resources,
                  Statistics statistics, ProjectLogger logger) {
        super(name);
        this.readyQueue = readyQueue;
        this.resources = resources;
        this.statistics = statistics;
        this.logger = logger;
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            try {
                Job job = readyQueue.take();
                if (job == null) {
                    break;
                }
                processJob(job);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void processJob(Job job) throws InterruptedException {
        statistics.markJobStarted();

        long startMs = logger.now();
        job.markStart(startMs);
        logger.jobStarted(job);

        if (job.workMs > 0L) {
            Thread.sleep(job.workMs);
        }
        logger.workFinished(job);

        if (job.resource != ResourceType.NONE) {
            long waitStarted = logger.now();
            job.markResourceWaitStarted(waitStarted);
            logger.resourceWaitStarted(job);
            try {
                resources.acquire(job.resource);
                long acquireTime = logger.now();
                job.markResourceWaitEnded(acquireTime);
                logger.resourceAcquired(job, acquireTime - waitStarted);
            } finally {
                // resource may have been acquired successfully or interrupted before acquire;
                // each worker handles its own acquisition and release responsibilities.
            }

            if (job.resourceMs > 0L) {
                Thread.sleep(job.resourceMs);
            }
            try {
                resources.release(job.resource);
                logger.resourceReleased(job);
            } finally {
                long completionMs = logger.now();
                job.markComplete(completionMs);
                statistics.recordCompletion(job);
                logger.jobCompleted(job);
            }
            return;
        }

        long completionMs = logger.now();
        job.markComplete(completionMs);
        statistics.recordCompletion(job);
        logger.jobCompleted(job);
    }
}
