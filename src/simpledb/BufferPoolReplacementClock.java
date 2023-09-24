package simpledb;

public class BufferPoolReplacementClock {

    public BufferPoolReplacementClock(int numEntries, boolean noSpin) {
        this.numEntries = numEntries;
        this.noSpin = noSpin;
    }

    public int next() throws DbException {
        var result = clock;
        clock = ++clock % numEntries;
        if (noSpin && result == searchStart) {
            throw new DbException("Clock value rolled over during search.");
        }
        return result;
    }

    public int current() {
        return clock;
    }

    public void start() {
        searchStart = clock;
    }

    private final int numEntries;
    private int clock = 0;
    private int searchStart;
    private final boolean noSpin;
}
