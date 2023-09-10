package simpledb;


import java.util.NoSuchElementException;

public class AggregatorScalarIterator implements DbIterator {

    public AggregatorScalarIterator(int aggregateVal) {
        scalarAggregateVal = aggregateVal;
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

        return !scalarRead;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!isOpen || !hasNext())
            throw new NoSuchElementException();
        IntField resultField = new IntField(scalarAggregateVal);
        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, resultField);
        scalarRead = true;
        return result;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        scalarRead = false;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] { Type.INT_TYPE } );
    }

    @Override
    public void close() {
        isOpen = false;
    }

    private Boolean isOpen = true;

    private Boolean scalarRead = false;
    private int scalarAggregateVal;
}
