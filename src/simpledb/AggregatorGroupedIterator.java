package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

public class AggregatorGroupedIterator implements DbIterator {

    public AggregatorGroupedIterator(HashMap<Field, AggregatorCounter> agg, Type gbfieldtype) {
        this.agg = agg;
        this.gbfieldtype = gbfieldtype;
        this.groupIterator = agg.entrySet().iterator();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        isOpen = true;
        this.rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!isOpen)
            throw new DbException("Iterrator is not open.");
        return groupIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!isOpen || !hasNext())
            throw new NoSuchElementException();
        Entry<Field, AggregatorCounter> nextEntry = groupIterator.next();
        int resultValue = nextEntry.getValue().GetWhat();
        IntField resultField = new IntField(resultValue);
        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, nextEntry.getKey());
        result.setField(1, resultField);
        return result;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.groupIterator = agg.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        if (gbfieldtype == Type.INT_TYPE) {
            return new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE });
        }
        else if (gbfieldtype == Type.STRING_TYPE) {
            return new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });
        }
        return null;
    }

    @Override
    public void close() {
        isOpen = false;
    }

    private Boolean isOpen = true;

    private HashMap<Field, AggregatorCounter> agg;
    private Iterator<Entry<Field, AggregatorCounter>> groupIterator;
    private Type gbfieldtype;
}
