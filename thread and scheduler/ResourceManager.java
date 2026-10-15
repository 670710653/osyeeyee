import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class ResourceManager {

    private final Map<ResourceType, Semaphore> semaphores = new EnumMap<>(ResourceType.class);
    private final int printerPermits;
    private final int databasePermits;

    public ResourceManager(int printerPermits, int databasePermits) {
        if (printerPermits < 1) {
            throw new IllegalArgumentException("printerPermits ต้องมีค่าตั้งแต่ 1");
        }
        if (databasePermits < 1) {
            throw new IllegalArgumentException("databasePermits ต้องมีค่าตั้งแต่ 1");
        }

        this.printerPermits = printerPermits;
        this.databasePermits = databasePermits;
        semaphores.put(ResourceType.PRINTER, new Semaphore(printerPermits, true));
        semaphores.put(ResourceType.DATABASE, new Semaphore(databasePermits, true));
    }

    public void acquire(ResourceType type) throws InterruptedException {
        if (type == null || type == ResourceType.NONE) {
            return;
        }
        Semaphore semaphore = semaphores.get(type);
        if (semaphore == null) {
            throw new IllegalArgumentException("ทรัพยากรไม่รู้จัก: " + type);
        }
        semaphore.acquire();
    }

    public void release(ResourceType type) {
        if (type == null || type == ResourceType.NONE) {
            return;
        }
        Semaphore semaphore = semaphores.get(type);
        if (semaphore != null) {
            semaphore.release();
        }
    }

    public String status() {
        int printerUsed = printerPermits - semaphores.get(ResourceType.PRINTER).availablePermits();
        int databaseUsed = databasePermits - semaphores.get(ResourceType.DATABASE).availablePermits();
        return String.format("printer=%d/%d database=%d/%d",
                printerUsed, printerPermits, databaseUsed, databasePermits);
    }
}
