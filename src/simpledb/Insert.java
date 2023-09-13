package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.t = t;
        this.child = child;
        this.tableid = tableid;
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
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        var numberOfInsertions = 0;
        if (burned) {
            return null;
        }
        try {
            while (child.hasNext()) {
                var tuple = child.next();
                Database.getBufferPool().insertTuple(t, tableid, tuple);
                numberOfInsertions++;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        var result = new Tuple(td);
        result.setField(0, new IntField(numberOfInsertions));
        burned = true;
        return result;
    }

    private static final TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });

    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private boolean burned = false;
}
