package simpledb;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        int tmpTupleCount = 0;
        try {
            var tableFile = Database.getCatalog().getDbFile(tableid);
            this.numPages = ((HeapFile)tableFile).numPages();
            var tableDesc = Database.getCatalog().getTupleDesc(tableid);
            this.fields = new TableStatsField[tableDesc.numFields()];
            for (int i = 0; i < tableDesc.numFields(); i++) {
                this.fields[i] = new TableStatsField(tableDesc.getType(i));
            }
            var it = tableFile.iterator(null);
            it.open();
            while (it.hasNext()) {
                tmpTupleCount++;
                var tuple = it.next();
                for (int i = 0; i < tableDesc.numFields(); i++) {
                    fields[i].processValueForRange(tuple.getField(i));
                }
            }
            for (int i = 0; i < tableDesc.numFields(); i++) {
                this.fields[i].commitRange(NUM_HIST_BINS);
            }
            it.rewind();
            while (it.hasNext()) {
                var tuple = it.next();
                for (int i = 0; i < tableDesc.numFields(); i++) {
                    fields[i].processValueForStats(tuple.getField(i));
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        tupleCount = tmpTupleCount;
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
        return ioCostPerPage * numPages;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)Math.round((double)tupleCount * selectivityFactor);        
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	return fields[field].estimateSelectivity(op, constant);
    }

    private TableStatsField[] fields;
    private final int ioCostPerPage;
    private int numPages;
    private final int tupleCount;
}
