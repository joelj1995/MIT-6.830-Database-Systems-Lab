package simpledb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class LockManagerTest {
    
    @Before public void setUp() throws Exception {
        this.lm = new LockManager();
    }

    @Test public void testAcquireLock() {
        var tid = new TransactionId();
        PageId pid = new HeapPageId(1, 1);
        var request = new LockManagerRequest(tid, pid.hashCode(), LockMode.S);
        var result = lm.acquireLock(request);
        assertTrue(result);
    }

    @Test public void testCanAcquireSharedLockAfterAnotherSharedLock() {
        var tid = new TransactionId();
        PageId pid = new HeapPageId(1, 1);
        var request = new LockManagerRequest(tid, pid.hashCode(), LockMode.S);
        var result = lm.acquireLock(request);
        assertTrue(result);
        request = new LockManagerRequest(tid, pid.hashCode(), LockMode.S);
        result = lm.acquireLock(request);
        assertTrue(result);
    }

    @Test public void testCannotAcquireSharedLockAfterExclusiveLock() {
        var tid = new TransactionId();
        var tid2 = new TransactionId();
        PageId pid = new HeapPageId(1, 1);
        var request = new LockManagerRequest(tid, pid.hashCode(), LockMode.X);
        var result = lm.acquireLock(request);
        assertTrue(result);
        request = new LockManagerRequest(tid2, pid.hashCode(), LockMode.S);
        result = lm.acquireLock(request);
        assertFalse(result);
    }

    @Test public void testTransactionCanUpgradeItsSharedLock() {
        var tid = new TransactionId();
        PageId pid = new HeapPageId(1, 1);
        var request = new LockManagerRequest(tid, pid.hashCode(), LockMode.S);
        var result = lm.acquireLock(request);
        assertTrue(result);
        request = new LockManagerRequest(tid, pid.hashCode(), LockMode.X);
        result = lm.acquireLock(request);
        assertTrue(result);
    }
    
    @Test public void testTransactionCanAcquireSharedLockIfHasXLock() {
        var tid = new TransactionId();
        PageId pid = new HeapPageId(1, 1);
        var request = new LockManagerRequest(tid, pid.hashCode(), LockMode.X);
        var result = lm.acquireLock(request);
        assertTrue(result);
        request = new LockManagerRequest(tid, pid.hashCode(), LockMode.X);
        result = lm.acquireLock(request);
        assertTrue(result);
    }

    @Test public void testReleaseLock() {
        var tid = new TransactionId();
        PageId pid = new HeapPageId(1, 1);
        var request = new LockManagerRequest(tid, pid.hashCode(), LockMode.X);
        var result = lm.acquireLock(request);
        assertTrue(result);
        lm.releaseLock(request);
        request = new LockManagerRequest(tid, pid.hashCode(), LockMode.S);
        result = lm.acquireLock(request);
        assertTrue(result);
    }

    @Test public void testHoldsLock() {
        var tid = new TransactionId();
        var tid2 = new TransactionId();
        PageId pid = new HeapPageId(1, 1);
        var request = new LockManagerRequest(tid, pid.hashCode(), LockMode.X);
        var result = lm.acquireLock(request);
        assertTrue(result);
        assertTrue(lm.holdsLock(tid, pid.hashCode()));
        assertFalse(lm.holdsLock(tid2, pid.hashCode()));
    }

    private LockManager lm;
}
