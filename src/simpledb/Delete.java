package simpledb;

import java.io.IOException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (burned) {
            return null;
        }
        var numberOfDeletions = 0;
        while (child.hasNext()) {
            var tuple = child.next();
            Database.getBufferPool().deleteTuple(t, tuple);
            numberOfDeletions++;
        }
        var result = new Tuple(td);
        result.setField(0, new IntField(numberOfDeletions));
        burned = true;
        return result;
    }

    private static final TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });

    private TransactionId t;
    private DbIterator child;
    private boolean burned = false;
}
