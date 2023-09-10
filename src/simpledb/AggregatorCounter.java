package simpledb;

public class AggregatorCounter {
    
    public AggregatorCounter(int start, Aggregator.Op what)
    {
        count = 1;
        sum = start;
        min = start;
        max = start;
        this.what = what;
    }

    public AggregatorCounter(String start, Aggregator.Op what)
    {
        if (!what.equals(Aggregator.Op.COUNT)) throw new IllegalArgumentException(what.toString());
        count = 1;
        this.what = what;
    }

    public void Next(int next)
    {
        count += 1;
        sum += next;
        min = Math.min(next, min);
        max = Math.max(next, min);
    }

    public void Next(String next)
    {
        if (!what.equals(Aggregator.Op.COUNT)) throw new IllegalArgumentException(next);
        count += 1;
    }

    public int GetWhat()
    {
        switch (what) {
            case COUNT:
                return GetCount();
            case SUM:
                return GetSum();
            case AVG:
                return GetAvg();
            case MIN:
                return GetMin();
            case MAX:
                return GetMax();
        }
        throw new UnsupportedOperationException("Op not recognized");
    }

    public int GetCount()
    {
        return count;
    }

    public int GetSum()
    {
        return sum;
    }

    public int GetAvg()
    {
        return sum / count;
    }

    public int GetMin()
    {
        return min;
    }

    public int GetMax()
    {
        return max;
    }

    private int count;
    private int sum;
    private int min;
    private int max;
    private Aggregator.Op what;
}
