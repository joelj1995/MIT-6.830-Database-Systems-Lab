package simpledb;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {

    /** Creates a new RecordId refering to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pid;
    }
    
    /**
     * Two RecordId objects are considered equal if they represent the same tuple.
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	if (o == null) return false;
        if (!(o instanceof RecordId)) return false;
        var recordId = (RecordId) o;
        return this.tupleno() == recordId.tupleno() && 
            this.getPageId().equals(recordId.getPageId());
    }
    
    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	var hash1 = Integer.hashCode(pid.pageno());
        var hash2 = Integer.hashCode(tupleno);
        var hash3 = Integer.hashCode(pid.getTableId());
        return Integer.hashCode(hash1 ^ hash2 ^ hash3);
    }
    
    private final PageId pid; 
    private final int tupleno;
}
