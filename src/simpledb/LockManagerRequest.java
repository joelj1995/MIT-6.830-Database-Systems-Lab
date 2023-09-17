package simpledb;

public class LockManagerRequest {

    public LockManagerRequest(TransactionId tid, Integer objectHash, LockMode mode) {
        this.tid = tid;
        this.objectHash = objectHash;
        this.mode = mode;
    }

    public static LockManagerRequest Shared(TransactionId tid, LockableObject obj) {
        return new LockManagerRequest(tid, obj.hashCode(), LockMode.S);
    }

    public static LockManagerRequest Exlusive(TransactionId tid, LockableObject obj) {
        return new LockManagerRequest(tid, obj.hashCode(), LockMode.X);
    }

    public static LockManagerRequest Release(TransactionId tid, LockableObject obj) {
        return new LockManagerRequest(tid, obj.hashCode(), LockMode.NL);
    }

    public String toString() {
        return "TID: " + tid.toString() + " Object: " + Integer.toHexString(objectHash) + " Mode: " + mode.toString();
    }

    public TransactionId transactionId() { return tid; }
    public Integer objectHash() { return objectHash; }
    public LockMode lockMode() { return mode; }

    private final TransactionId tid;
    private final Integer objectHash;
    private final LockMode mode;
}
