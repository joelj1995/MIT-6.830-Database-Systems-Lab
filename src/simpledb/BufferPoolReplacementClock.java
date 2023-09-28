package simpledb;

public class BufferPoolReplacementClock {

    public BufferPoolReplacementClock(int numEntries, boolean noSpin) {
        this.numEntries = numEntries;
        this.noRollOver = noSpin;
    }

    public int next() throws DbException {
        var result = clock;
        clock = ++clock % numEntries;
        if (noRollOver && result == searchStart) {
            if (firstPass) firstPass = false;
            else throw new DbException("Clock value rolled over during search.");
        }
        return result;
    }

    public int current() {
        return clock;
    }

    public void start() {
        searchStart = clock;
        firstPass = true;
    }

    private final int numEntries;
    private int clock = 0;
    private int searchStart;
    private final boolean noRollOver;
    private boolean firstPass = false;
}
