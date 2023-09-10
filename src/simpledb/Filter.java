package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {

    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.predicate = p;
        this.childIterator = child;
    }

    public TupleDesc getTupleDesc() {
        return childIterator.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        childIterator.open();
    }

    public void close() {
        childIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        childIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext()
            throws NoSuchElementException, TransactionAbortedException, DbException {
        try {
            Tuple next;
            do {
                next = childIterator.next();
            } while (!predicate.filter(next));
            return next;
        } catch (NoSuchElementException e) {
            return null;
        }
    }
    
    private Predicate predicate;
    private DbIterator childIterator;
}
