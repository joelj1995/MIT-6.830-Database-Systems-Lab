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
        if (numPages > 0 && curPageNo + 1 < numPages)
            return true;
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
        curPageNo++;
        PageId pageToRead = new HeapPageId(fileId, curPageNo);
        HeapPage curPage = (HeapPage) bp.getPage(tid, pageToRead, Permissions.READ_ONLY);
        curPageIterator = curPage.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        curPageIterator = null;
        curPageNo = -1;
        nextPage();
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
