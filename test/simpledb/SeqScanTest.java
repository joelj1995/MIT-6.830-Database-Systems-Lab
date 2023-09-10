package simpledb;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;

public class SeqScanTest extends SimpleDbTestBase {

    @Before public void addTables() throws Exception {
        Database.getCatalog().clear();
        var td = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE }, new String[] { "one", "two" });
        Database.getCatalog().addTable(new SkeletonFile(-1, td), name);
        TransactionId tid = new TransactionId();
        this.seqScan = new SeqScan(tid, -1, tableAlias);
    }
    
    @Test public void getTupleDesc() throws Exception {
        TupleDesc expected = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE }, new String[] { "froo.one", "froo.two" });
        TupleDesc actual = seqScan.getTupleDesc();
        for (int i = 0; i < expected.numFields(); i++) { // fields match
            assertEquals(expected.getFieldName(i), actual.getFieldName(i));
        }
        assertEquals(expected, actual); // types match
    }

    private static String name = "floo";
    private static String tableAlias = "froo";
    private SeqScan seqScan;
}
