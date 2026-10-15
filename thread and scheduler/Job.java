/**
 * ข้อมูลของงานหนึ่งชิ้น
 */
public class Job {

    public final String id;
    public final long arrivalMs;
    public final int priority;
    public final long workMs;
    public final ResourceType resource;
    public final long resourceMs;
    public final int sequence;

    private volatile long actualArrivalMs = -1L;
    private volatile long actualStartMs = -1L;
    private volatile long completionMs = -1L;
    private volatile long resourceWaitMs = 0L;
    private volatile long resourceWaitStartedMs = -1L;

    public Job(String id, long arrivalMs, int priority, long workMs,
               ResourceType resource, long resourceMs, int sequence) {
        this.id = id;
        this.arrivalMs = arrivalMs;
        this.priority = priority;
        this.workMs = workMs;
        this.resource = resource;
        this.resourceMs = resourceMs;
        this.sequence = sequence;
    }

    public void markActualArrival(long nowMs) {
        this.actualArrivalMs = nowMs;
    }

    public void markStart(long nowMs) {
        this.actualStartMs = nowMs;
    }

    public void markResourceWaitStarted(long nowMs) {
        this.resourceWaitStartedMs = nowMs;
    }

    public void markResourceWaitEnded(long nowMs) {
        if (resourceWaitStartedMs >= 0L) {
            this.resourceWaitMs += nowMs - resourceWaitStartedMs;
            this.resourceWaitStartedMs = -1L;
        }
    }

    public void markComplete(long nowMs) {
        this.completionMs = nowMs;
    }

    public long actualArrivalMs() {
        return actualArrivalMs;
    }

    public long actualStartMs() {
        return actualStartMs;
    }

    public long completionMs() {
        return completionMs;
    }

    public long waitingTimeMs() {
        if (actualArrivalMs < 0L || actualStartMs < 0L) {
            return 0L;
        }
        return actualStartMs - actualArrivalMs;
    }

    public long turnaroundTimeMs() {
        if (actualArrivalMs < 0L || completionMs < 0L) {
            return 0L;
        }
        return completionMs - actualArrivalMs;
    }

    public long resourceWaitTimeMs() {
        return resourceWaitMs;
    }

    public long resourceWaitStartedMs() {
        return resourceWaitStartedMs;
    }

    @Override
    public String toString() {
        return String.format("%s(priority=%d, work=%dms, %s)",
                id, priority, workMs,
                resource == ResourceType.NONE ? "no resource"
                        : resource + " " + resourceMs + "ms");
    }
}
