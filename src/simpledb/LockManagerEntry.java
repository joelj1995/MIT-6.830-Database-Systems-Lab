package simpledb;

public class LockManagerEntry {

    public LockManagerEntry(LockMode mode, Integer objectHash, TransactionId owner) {
        this.mode = mode;
        this.objectHash = objectHash;
        this.owner = owner;
        this.callStackAtCreation = getStack();
        this.threadId = Thread.currentThread().threadId();
    }

    public LockManagerEntry(LockManagerRequest request) {
        this.mode = request.lockMode();
        this.objectHash = request.objectHash();
        this.owner = request.transactionId();
        this.callStackAtCreation = getStack();
        this.threadId = Thread.currentThread().threadId();
    }

    public String toString() {
        return "Mode: " + mode.toString() + " Object: " + Integer.toHexString(objectHash) + " Owner: " + owner.toString() + " Thread: " + threadId + callStackAtCreation;
    }

    private String getStack() {
        var stack = "\n";
        for (var element : Thread.currentThread().getStackTrace()) {
            if (!element.toString().startsWith("simpledb")) continue;
            stack += element.toString() + '\n';
        }
        return stack;
    }

    public LockMode mode() { return mode; }
    public Integer objectHash() { return objectHash; }
    public TransactionId owner() { return owner; }
    public final String callStackAtCreation;
    
    private final LockMode mode;
    private final Integer objectHash;
    private final TransactionId owner;
    private final long threadId;
}
