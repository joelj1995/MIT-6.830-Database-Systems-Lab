package simpledb;

public class BufferPoolPageEntry {

    public BufferPoolPageEntry(int hash, int index) {
        this.hash = hash;
        this.index = index;
        pinCount = 0;
        referenced = false;
    }

    public int hash() {
        return hash;
    }

    public int index() {
        return index;
    }

    public int pinCount() {
        return pinCount;
    }

    public void pin() {
        pinCount++;
    }

    public void unpin() {
        pinCount--;
    }

    public Boolean referenced() {
        return referenced;
    }

    public void setReferenced(Boolean r) {
        referenced = r;
    }

    public String toString() {
        return "Hash: " + Integer.toHexString(hash) + " Index: " + index;
    }

    private int hash;
    private int index;
    private int pinCount;
    private Boolean referenced;
}
