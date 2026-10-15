import java.util.Comparator;
import java.util.PriorityQueue;

public class ReadyQueue {

    private final Config.Policy policy;
    private final PriorityQueue<Job> queue;
    private final Object monitor = new Object();
    private boolean closed = false;

    public ReadyQueue(Config.Policy policy) {
        this.policy = policy;
        this.queue = new PriorityQueue<>(11, comparatorFor(policy));
    }

    public void add(Job job) {
        synchronized (monitor) {
            if (closed) {
                throw new IllegalStateException("ไม่สามารถใส่งานลง ReadyQueue หลังปิดคิวแล้ว");
            }
            queue.add(job);
            monitor.notifyAll();
        }
    }

    public Job take() throws InterruptedException {
        synchronized (monitor) {
            while (queue.isEmpty() && !closed) {
                monitor.wait();
            }
            if (!queue.isEmpty()) {
                return queue.poll();
            }
            return null;
        }
    }

    public void close() {
        synchronized (monitor) {
            closed = true;
            monitor.notifyAll();
        }
    }

    public int size() {
        synchronized (monitor) {
            return queue.size();
        }
    }

    private static Comparator<Job> comparatorFor(Config.Policy policy) {
        if (policy == Config.Policy.FCFS) {
            return Comparator.<Job>comparingLong(job -> job.arrivalMs)
                    .thenComparingInt(job -> job.sequence)
                    .thenComparing(job -> job.id);
        }
        return Comparator.<Job>comparingInt(job -> job.priority)
                .thenComparingInt(job -> job.sequence)
                .thenComparing(job -> job.id);
    }
}
