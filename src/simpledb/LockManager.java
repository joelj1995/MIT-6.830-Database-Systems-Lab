package simpledb;

import java.util.ArrayList;
import java.util.List;

public class LockManager {
    
    public synchronized boolean acquireLock(LockManagerRequest request) {
        var existingLocks = findEntriesByObjectId(request.objectHash());
        for (LockManagerEntry existingLock : existingLocks) {
            if (existingLock.owner().equals(request.transactionId())) {
                if (existingLock.mode().equals(request.lockMode())) {
                    return true;
                }
                else {
                    continue;
                }
            }
            var isCompatible = testCompatability(request.lockMode(), existingLock.mode());
            if (!isCompatible) return false;
        }
        locks.add(new LockManagerEntry(request));
        return true;
    }

    public void waitForLock(LockManagerRequest request) throws DbException, TransactionAbortedException {
        int wait = 0;
        while (true) {
            try {
                if (acquireLock(request)) return;
                Thread.sleep(DbConfig.lockWaitMs);
                wait += DbConfig.lockWaitMs;
                if (wait > DbConfig.maxLockWaitMs) {
                    throw new TransactionAbortedException();
                }
            }
            catch (InterruptedException e) {
                throw new DbException("Process was interrupted while waiting for lock.");
            }
        }
    }

    public synchronized void releaseLock(LockManagerRequest request) {
        var toRemove = new ArrayList<LockManagerEntry>();
        if (request.objectHash() == null) {
            for (LockManagerEntry existingLock : locks) {
                if (existingLock.owner().equals(request.transactionId())) toRemove.add(existingLock); 
            }
        }
        else {
            var existingLocks = findEntriesByObjectId(request.objectHash());
            for (LockManagerEntry existingLock : existingLocks) {
                if (existingLock.owner().equals(request.transactionId())) toRemove.add(existingLock); 
            }
        }
        for (var item : toRemove) {
            locks.remove(item);
        }
    }

    public synchronized boolean holdsLock(TransactionId tid, Integer obj) {
        var existingLocks = findEntriesByObjectId(obj);
        for (LockManagerEntry existingLock : existingLocks) {
            if (existingLock.owner().equals(tid)) return true; 
        }
        return false;
    }

    public String toString() {
        var result = "";
        for (LockManagerEntry lock : locks) {
            result += lock.toString() + "\n";
        }
        return result;
    }

    private boolean testCompatability(LockMode lock1, LockMode lock2) {
        if (lock1.equals(LockMode.S)) {
            if (lock2.equals(LockMode.S)) return true;
            if (lock2.equals(LockMode.X)) return false;
        }
        else if (lock1.equals(LockMode.X)) {
            if (lock2.equals(LockMode.S)) return false;
            if (lock2.equals(LockMode.X)) return false;
        }
        return false;
    }

    private List<LockManagerEntry> findEntriesByObjectId(int hash) {
        var result = new ArrayList<LockManagerEntry>();
        for (LockManagerEntry entry : locks) {
            if (entry.objectHash().equals(hash)) result.add(entry);
        }
        return result;
    }

    private final List<LockManagerEntry> locks = new ArrayList<LockManagerEntry>();
}
