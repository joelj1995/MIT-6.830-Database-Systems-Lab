package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId hpid = (HeapPageId) pid;
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");            
            byte[] pageData = new byte[BufferPool.PAGE_SIZE];
            int offset = pid.pageno() * BufferPool.PAGE_SIZE;
            raf.seek(offset);
            if (raf.read(pageData, 0, BufferPool.PAGE_SIZE) == -1) {
                raf.close();
                throw new IllegalArgumentException(pid.toString());
            }
            HeapPage page = new HeapPage(hpid, pageData);
            raf.close();
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null; // should not get here
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek(page.getId().pageno() * BufferPool.PAGE_SIZE);
            raf.write(page.getPageData(), 0, BufferPool.PAGE_SIZE);
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)(f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        var pageForInsert = getPageForInsert(tid);
        pageForInsert.addTuple(t);
        var result = new ArrayList<Page>();
        result.add(pageForInsert);
        return result;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        var iTuples = iterator(tid);
        iTuples.open();
        while (iTuples.hasNext()) {
            var nextTuple = iTuples.next();
            if (nextTuple.equals(t)) {
                var pageId = nextTuple.getRecordId().getPageId();
                iTuples.close();
                var page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                page.deleteTuple(t);
                return page;
            }
        }
        throw new DbException("Tuple not found.");
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private HeapPage getPageForInsert(TransactionId tid) throws IOException, TransactionAbortedException, DbException {
        var lastPageId = new HeapPageId(getId(), numPages() - 1);
        var page = (HeapPage)Database.getBufferPool().getPage(tid, lastPageId, Permissions.READ_WRITE);

        if (page.getNumEmptySlots() > 0) return page;

        var newPageId = lastPageId.forward();
        var newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
        writePage(newPage);
        return (HeapPage)Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
    }
    
    private final File f;
    private final TupleDesc td;
}

