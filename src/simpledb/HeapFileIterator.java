package simpledb;

import java.io.Console;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.tid = tid;
        bp = Database.getBufferPool();
        numPages = hf.numPages();
        fileId = hf.getId();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        isOpen = true;
        rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!isOpen)
            return false;
        if (curPageNo == -1) return false;
        if (curPageNo + 1 < numPages) {
            for (int i = curPageNo + 1; i < numPages; i++) {
                PageId pageToPeak = new HeapPageId(fileId, i);
                HeapPage peakPage = (HeapPage) bp.getPage(tid, pageToPeak, Permissions.READ_ONLY);
                if (peakPage.getNumEmptySlots() < peakPage.numSlots) return true;
            }
        }
        return curPageIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!isOpen || numPages == 0)
            throw new NoSuchElementException();
        if (curPageIterator == null || !curPageIterator.hasNext()) {
            if (curPageNo + 1 < numPages) {
                nextPage();
            }
            else {
                throw new NoSuchElementException();
            }
        }
        return curPageIterator.next();
    }

    private void nextPage() throws TransactionAbortedException, DbException   {
        HeapPage curPage = null;

        do {
            curPageNo++;
            if (curPageNo >= numPages) throw new NoSuchElementException();
            PageId pageToRead = new HeapPageId(fileId, curPageNo);
            if (pageToRead.pageno() == 10) {
                var foo = 10;
            }
            curPage = (HeapPage) bp.getPage(tid, pageToRead, Permissions.READ_ONLY);
            if (!curPage.getId().equals(pageToRead)) 
                throw new DbException("The expected page was not read." + curPage.getId().hashCode() + " " + pageToRead.hashCode());
        } while (curPage.getNumEmptySlots() == curPage.numSlots);
        
        curPageIterator = curPage.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        curPageIterator = null;
        curPageNo = -1;
        try {
            nextPage();
        } catch(NoSuchElementException e) {
            curPageNo = -1;
        }
    }

    @Override
    public void close() {
        isOpen = false;
    }
    
    private Boolean isOpen = false;
    private BufferPool bp;
    private TransactionId tid;
    private int numPages;
    private int fileId;
    private int curPageNo = -1;
    private Iterator<Tuple> curPageIterator;
}
