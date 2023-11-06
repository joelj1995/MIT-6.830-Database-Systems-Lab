package simpledb;

import org.junit.Test;
import org.junit.Assert;

import simpledb.Predicate.Op;

public class IntHistogramTestJoel {

    @Test public void testGetBin() {
        /*
         *  0          1       2       3       4
         * [10 11 12] [13 14] [15 16] [17 18] [19 20]
         */
        var histogram = new IntHistogram(5, 10, 20);

        Assert.assertEquals(0, histogram.getBin(10));
        Assert.assertEquals(0, histogram.getBin(11));
        Assert.assertEquals(0, histogram.getBin(12));

        Assert.assertEquals(1, histogram.getBin(13));
        Assert.assertEquals(1, histogram.getBin(14));

        Assert.assertEquals(2, histogram.getBin(15));
        Assert.assertEquals(2, histogram.getBin(16));

        Assert.assertEquals(3, histogram.getBin(17));
        Assert.assertEquals(3, histogram.getBin(18));

        Assert.assertEquals(4, histogram.getBin(19));
        Assert.assertEquals(4, histogram.getBin(20));
    }

    @Test public void testGreaterThanSelectivityBasic() {
        var histogram = new IntHistogram(5, 10, 20);

        var c = 1;
        for (int i = 0; i < c; i++) {
            histogram.addValue(19);
        }
        
        Assert.assertEquals(1.0, histogram.estimateSelectivity(Op.GREATER_THAN, 18), 0.01);
    }

    @Test public void testGreaterThanSelectivityHarder() {
        /*
         *   0             1             2       
         *  [10 11 12 13] [14 15 16 17] [18 19 20 21] 
         */
        var histogram = new IntHistogram(3, 10, 21);

        var c = 8;
        for (int i = 0; i < c; i++) {
            histogram.addValue(17);
        }
        
        Assert.assertEquals(0.5, histogram.estimateSelectivity(Op.GREATER_THAN, 15), 0.01);
    }

    @Test public void testLessThanSelectivityHarder() {
        /*
         *   0             1             2       
         *  [10 11 12 13] [14 15 16 17] [18 19 20 21] 
         */
        var histogram = new IntHistogram(3, 10, 21);

        var c = 8;
        for (int i = 0; i < c; i++) {
            histogram.addValue(17);
        }
        
        Assert.assertEquals(0.5, histogram.estimateSelectivity(Op.LESS_THAN, 16), 0.01);
    }

    @Test public void testLessThanOrEqualSelectivityHarder() {
        /*
         *   0             1             2       
         *  [10 11 12 13] [14 15 16 17] [18 19 20 21] 
         */
        var histogram = new IntHistogram(3, 10, 21);

        var c = 8;
        for (int i = 0; i < c; i++) {
            histogram.addValue(17);
        }
        
        Assert.assertEquals(0.5, histogram.estimateSelectivity(Op.LESS_THAN_OR_EQ, 15), 0.01);
    }

    @Test public void testGreaterThanOrEqualSelectivityHarder() {
        /*
         *   0             1             2       
         *  [10 11 12 13] [14 15 16 17] [18 19 20 21] 
         */
        var histogram = new IntHistogram(3, 10, 21);

        var c = 8;
        for (int i = 0; i < c; i++) {
            histogram.addValue(17);
        }
        
        Assert.assertEquals(0.5, histogram.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 16), 0.01);
    }

    @Test public void testBinWidth() {
        /*
         *  0          1       2       3       4
         * [10 11 12] [13 14] [15 16] [17 18] [19 20]
         */
        var histogram = new IntHistogram(5, 10, 20);

        Assert.assertEquals(3, histogram.binWidth(0));
        Assert.assertEquals(2, histogram.binWidth(1));
        Assert.assertEquals(2, histogram.binWidth(2));
        Assert.assertEquals(2, histogram.binWidth(3));
        Assert.assertEquals(2, histogram.binWidth(4));
    }
    
    @Test public void testBinRight() {
        /*
         *  0          1       2       3       4
         * [10 11 12] [13 14] [15 16] [17 18] [19 20]
         */
        var histogram = new IntHistogram(5, 10, 20);

        Assert.assertEquals(12, histogram.binRight(0));
        Assert.assertEquals(14, histogram.binRight(1));
        Assert.assertEquals(16, histogram.binRight(2));
        Assert.assertEquals(18, histogram.binRight(3));
        Assert.assertEquals(20, histogram.binRight(4));
    }

    @Test public void testBinLeft() {
        /*
         *  0          1       2       3       4
         * [10 11 12] [13 14] [15 16] [17 18] [19 20]
         */
        var histogram = new IntHistogram(5, 10, 20);

        Assert.assertEquals(10, histogram.binLeft(0));
        Assert.assertEquals(13, histogram.binLeft(1));
        Assert.assertEquals(15, histogram.binLeft(2));
        Assert.assertEquals(17, histogram.binLeft(3));
        Assert.assertEquals(19, histogram.binLeft(4));
    }
}
