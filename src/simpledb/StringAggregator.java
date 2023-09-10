package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (!what.equals(Op.COUNT)) throw new IllegalArgumentException(what.toString());
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.curAg = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field groupingKey = gbfield == NO_GROUPING ? noGroupingField : tup.getField(gbfield);
        String curValue = ((StringField)tup.getField(afield)).getValue();
        if (!curAg.containsKey(groupingKey)) {
            curAg.put(groupingKey, new AggregatorCounter(curValue, what));
        }
        else {
            curAg.get(groupingKey).Next(curValue);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        if (gbfield == NO_GROUPING) {
            int aggregateVal = curAg.get(noGroupingField).GetWhat();
            return new AggregatorScalarIterator(aggregateVal);
        }
        else {
            return new AggregatorGroupedIterator(curAg, gbfieldtype);
        }
    }

    private static Field noGroupingField = new StringField("DEFAULT", 10);

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private HashMap<Field, AggregatorCounter> curAg;
}
