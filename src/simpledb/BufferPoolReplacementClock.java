package simpledb;

public class BufferPoolReplacementClock {

    public BufferPoolReplacementClock(int numEntries) {
        this.numEntries = numEntries;
    }

    public int next() {
        var result = clock;
        clock = ++clock % numEntries;
        return result;
    }

    public int current() {
        return clock;
    }

    private final int numEntries;
    private int clock = 0;
}
