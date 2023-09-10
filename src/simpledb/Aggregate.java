package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        var afieldType = child.getTupleDesc().getType(afield);
        var gfieldType = child.getTupleDesc().getType(gfield);
        var gfieldName = child.getTupleDesc().getFieldName(gfield);
        switch (aop) {
            case MIN:
            case MAX:
            case AVG:
            case SUM:
                this.aggregator = new IntAggregator(gfield, gfieldType, afield, aop);
            case COUNT:
                if (afieldType.equals(Type.INT_TYPE)) this.aggregator = new IntAggregator(gfield, gfieldType, afield, aop);
                else this.aggregator = new StringAggregator(gfield, gfieldType, afield, aop);
        }
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        if (gfield == Aggregator.NO_GROUPING) {
            this.td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { aggName(aop) });
        } else {
            this.td = new TupleDesc(new Type[] { afieldType, Type.INT_TYPE }, new String[] { gfieldName, aggName(aop) });
        }
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
            aggregator.merge(child.next());
        }
        aggregatorIterator = aggregator.iterator();
        aggregatorIterator.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (aggregatorIterator.hasNext())
            return aggregatorIterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggregatorIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
        aggregatorIterator.close();
    }

    DbIterator child;
    DbIterator aggregatorIterator;
    Aggregator aggregator;
    int afield;
    int gfield;
    Aggregator.Op aop;
    TupleDesc td;
}
