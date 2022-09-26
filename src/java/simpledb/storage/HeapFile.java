package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;
    private int tableId;
    // private HashMap<PageId, Page> pages;
    private ReentrantLock lock;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.tableId = f.getAbsolutePath().hashCode();
        this.lock = new ReentrantLock();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getClass() != HeapPageId.class) {
            System.out.println("wrong pageid class");
            return null;
        }
        HeapPageId hpid = (HeapPageId) pid;
        try (FileInputStream stream = new FileInputStream(this.file)) {
            lock.lock();
            HeapPage currentPage = null;
            byte[] bytes = new byte[BufferPool.getPageSize()];
            System.out.printf("bytes length: %d, off %d, len %d\n", bytes.length,
                    pid.getPageNo() * BufferPool.getPageSize(), BufferPool.getPageSize());
            // int res = stream.read(bytes, pid.getPageNumber() * BufferPool.getPageSize(), BufferPool.getPageSize());
            stream.skip(pid.getPageNo() * BufferPool.getPageSize());
            int res = stream.read(bytes, 0, BufferPool.getPageSize());
            if (res == -1) {
                System.out.printf("unable to read stream, pageNo %d", pid.getPageNo());
                return null;
            }
            currentPage = new HeapPage(hpid, bytes);
            return currentPage;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil((1.0 * this.file.length()) / (1.0 * BufferPool.getPageSize()));
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        HeapFileIterator iterator = new HeapFileIterator(this.tableId);
        return iterator;
    }

}

class HeapFileIterator extends AbstractDbFileIterator {
    private int tableId;
    private int currentPageNumber = 0;
    private HeapPage currentPage = null;
    private HeapPageId currentPageId = null;
    private Iterator<Tuple> currentIterator = null;
    private Boolean closeFlag = false;
    private BufferPool bufferPool = Database.getBufferPool();
    public HeapFileIterator(int tableId) {
        this.tableId = tableId;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        closeFlag = false;
        currentPageNumber = 0;
        currentPageId = new HeapPageId(tableId, currentPageNumber);
        currentPage = (HeapPage) bufferPool.getPage(null, currentPageId, null);
        if (currentPage == null) {
            System.out.println("open fail, no page");
            throw  new DbException("open fail, no page");
        }
        currentIterator = currentPage.iterator();
        currentPageNumber++;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        closeFlag = false;
        currentPageNumber = 0;
        currentPageId = new HeapPageId(tableId, currentPageNumber);
        currentPage = (HeapPage) bufferPool.getPage(null, currentPageId, null);
        if (currentPage == null) {
            System.out.println("rewind fail, no page");
            throw  new DbException("rewind fail, no page");
        }
        currentIterator = currentPage.iterator();
        currentPageNumber++;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (closeFlag) {
            return null;
        }
        // not open
        if (currentIterator == null) {
            System.out.println("currentIterator is null");
            return null;
        }
        // if current page still has next
        if (currentIterator.hasNext()) {
            return currentIterator.next();
        }
        // current page do not has next
        // read new page
        currentPageId = new HeapPageId(this.tableId, currentPageNumber);
        currentPage = (HeapPage) bufferPool.getPage(null, currentPageId, null);
        if (currentPage == null) {
            // no new page
            System.out.printf("readNext fail, currentPageNumber %d has no page", currentPageNumber);
            return null;
        }
        currentPageNumber++;
        currentIterator = currentPage.iterator();
        if (currentIterator.hasNext()) {
            return currentIterator.next();
        }
        // currentPage is an empty page
        return null;
    }
    @Override
    public void close() {
        super.close();
        closeFlag = true;
    }

}