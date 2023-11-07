package simpledb;

import java.security.InvalidParameterException;
import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        if (buckets < 1) {
            throw new InvalidParameterException();
        }
        if (min > max) {
            throw new InvalidParameterException();
        }
        bins = new int[buckets];
        this.min = min;
        this.max = max;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v < min) {
            throw new InvalidParameterException();
        }
        if (v > max) {
            throw new InvalidParameterException();
        }
        bins[getBin(v)]++;
        total++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        var bin = getBin(v);
        var estimatedCount = 0.0f;
        switch (op) {
            case EQUALS:
                if (bin < firstBin() || bin > lastBin()) {
                    return 0.0;
                }
                return (float)bins[bin] / total;
            case GREATER_THAN: // f > const
                if (bin < firstBin()) {
                    return 1.0;
                }
                if (bin > lastBin()) {
                    return 0.0;
                }
                // walk bins above v
                for (int i = bin + 1; i < bins.length; i++) {
                    estimatedCount += bins[i];
                }
                // add the fraction estimated in the bucket for v
                estimatedCount += ((float)(binRight(bin) - v) / binWidth(bin)) * total;
                return estimatedCount / total;
            case LESS_THAN:
                if (bin < firstBin()) {
                    return 0.0;
                }
                if (bin > lastBin()) {
                    return 1.0;
                }
                // walk bins below v
                for (int i = 0; i < bin; i++) {
                    estimatedCount += bins[i];
                }
                // add the fraction estimated in the bucket for v
                estimatedCount += ((float)(v - binLeft(bin)) / binWidth(bin)) * total;
                return estimatedCount / total;
            case LESS_THAN_OR_EQ:
                v++;
                bin = getBin(v);
                if (bin < firstBin()) {
                    return 0.0;
                }
                if (bin > lastBin()) {
                    return 1.0;
                }
                // walk bins below v
                for (int i = 0; i < bin; i++) {
                    estimatedCount += bins[i];
                }
                // add the fraction estimated in the bucket for v
                estimatedCount += ((float)(v - binLeft(bin)) / binWidth(bin)) * total;
                return estimatedCount / total;
            case GREATER_THAN_OR_EQ: // f > const
                v--;
                bin = getBin(v);
                if (bin < firstBin()) {
                    return 1.0;
                }
                if (bin > lastBin()) {
                    return 0.0;
                }
                // walk bins above v
                for (int i = bin + 1; i < bins.length; i++) {
                    estimatedCount += bins[i];
                }
                // add the fraction estimated in the bucket for v
                estimatedCount += ((float)(binRight(bin) - v) / binWidth(bin)) * total;
                return estimatedCount / total;
            case NOT_EQUALS:
                if (bin < firstBin() || bin > lastBin()) {
                    return 1.0;
                }
                return 1 - (float)bins[bin] / total;
            default:
                throw new UnsupportedOperationException();
        }
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }

    public int getBin(int v) {
        if (endToEndWidth() < bins.length) {
            return v - min;
        }
        return (int)((
                (float)(v - min) 
                / 
                (max - min + 1)
            ) * bins.length);
    }

    public int firstBin() {
        return 0;
    }

    public int lastBin() {
        return Math.min(bins.length - 1, endToEndWidth()-1);
    }

    public int binWidth(int i) {
        var width = (endToEndWidth() / bins.length) +
            (i + 1 <= endToEndWidth() % bins.length ? 
                1 : 0);
        return width;
    }

    public int binRight(int i) {
        return min 
            + (endToEndWidth() / bins.length) * (i + 1) 
            + Math.min(endToEndWidth() % bins.length, i + 1) 
            - 1;
    }

    public int binLeft(int i) {
        return min 
            + (endToEndWidth() / bins.length) * i 
            + Math.min(endToEndWidth() % bins.length, i);
    }

    private int endToEndWidth() {
        return max - min + 1;
    }

    private int total = 0;

    private int[] bins;
    private final int min; 
    private final int max;
}
