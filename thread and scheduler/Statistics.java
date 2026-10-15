import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Statistics {

    private final List<Job> completedJobs = new ArrayList<>();
    private final AtomicInteger runningJobs = new AtomicInteger(0);

    public void markJobStarted() {
        runningJobs.incrementAndGet();
    }

    public synchronized void recordCompletion(Job job) {
        if (job == null) {
            return;
        }
        completedJobs.add(job);
        runningJobs.decrementAndGet();
    }

    public int completedCount() {
        synchronized (completedJobs) {
            return completedJobs.size();
        }
    }

    public int runningCount() {
        return runningJobs.get();
    }

    public void printSummary(List<Job> allJobs, long makespanMs) {
        if (allJobs == null || allJobs.isEmpty()) {
            System.out.println("No jobs processed.");
            return;
        }

        double avgWaiting = allJobs.stream().mapToLong(Job::waitingTimeMs).average().orElse(0.0);
        double avgTurnaround = allJobs.stream().mapToLong(Job::turnaroundTimeMs).average().orElse(0.0);
        double avgResourceWait = allJobs.stream()
                .filter(job -> job.resource != ResourceType.NONE)
                .mapToLong(Job::resourceWaitTimeMs)
                .average()
                .orElse(0.0);
        double throughput = allJobs.size() / (makespanMs > 0L ? (makespanMs / 1000.0) : 1.0);

        System.out.println("\n=== Summary ===");
        System.out.printf("avg Waiting Time: %.0f ms%n", avgWaiting);
        System.out.printf("avg Turnaround Time: %.0f ms%n", avgTurnaround);
        System.out.printf("avg Resource Wait: %.0f ms%n", avgResourceWait);
        System.out.printf("Throughput: %.2f jobs/sec%n", throughput);
        System.out.printf("makespan: %d ms%n", makespanMs);
    }
}
