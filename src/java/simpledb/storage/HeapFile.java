package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static simpledb.common.Permissions.READ_ONLY;
import static simpledb.common.Permissions.READ_WRITE;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;
    private int tableId;
    // private HashMap<PageId, Page> pages;
    private ReentrantLock lock; //file lock

    private BufferPool bufferPool ;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.tableId = f.getAbsolutePath().hashCode();
        this.lock = new ReentrantLock();
        this.bufferPool = Database.getBufferPool();
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
        //System.out.printf("page size: %d, file size %d\n", BufferPool.getPageSize(),
        //    this.file.length());
        // some code goes here
        if (pid.getClass() != HeapPageId.class) {
            System.out.println("wrong page id class");
            return null;
        }
        if ((long) pid.getPageNumber() * BufferPool.getPageSize() >= this.file.length()) {
            throw new IllegalArgumentException();
        }
        try (RandomAccessFile raf = new RandomAccessFile(this.file, "r")) {
            HeapPageId hpid = (HeapPageId) pid;
            lock.lock();
            byte[] bytes = new byte[BufferPool.getPageSize()];
            //System.out.printf("bytes length: %d, off %d, len %d\n", bytes.length,
            //        pid.getPageNumber() * BufferPool.getPageSize(), BufferPool.getPageSize());
            raf.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            int res = raf.read(bytes);
            if (res == -1) {
                System.out.printf("unable to read stream, pageNo %d\n", pid.getPageNumber());
                return null;
            }
            return new HeapPage(hpid, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {

        try (RandomAccessFile raf = new RandomAccessFile(this.file, "rw")) {
            lock.lock();
            raf.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
            raf.write(page.getPageData());
        } finally {
            lock.unlock();
        }
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
        // find a page having empty slots
        for (int i = 0; i < numPages(); i++) {
            HeapPage hp = (HeapPage) bufferPool
                    .getPage(tid, new HeapPageId(this.tableId, i), Permissions.READ_WRITE);
            if (hp.getNumEmptySlots() != 0) {
                System.out.println("inserted page: " + hp);
                System.out.println("bufferpool " + bufferPool);
                hp.insertTuple(t);
                hp.markDirty(true, tid);
                return List.of(hp);
            }
            bufferPool.releasePage(tid, hp.pid, READ_WRITE);
        }
        // if no page having empty slots, create a page and write page
        HeapPageId newHpid = new HeapPageId(this.tableId, numPages());
        HeapPage newHp = new HeapPage(newHpid, HeapPage.createEmptyPageData());
        writePage(newHp);
        newHp = (HeapPage) bufferPool.getPage(tid, newHpid, READ_WRITE);
        newHp.insertTuple(t);
        newHp.markDirty(true, tid);
        return List.of(newHp);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        HeapPage hp = (HeapPage) bufferPool
                .getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        hp.deleteTuple(t);
        return new ArrayList<Page>() {{
            add(hp);
        }};
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this.tableId, numPages());
    }

    public String toString() {
        return "tableId: " + this.tableId + "file: " + this.file.toString();
    }
}

class HeapFileIterator extends AbstractDbFileIterator {
    private int tableId;
    private int maxPageNumber;

    private TransactionId tid;
    private int currentPageNumber = 0;
    private HeapPage currentPage = null;
    private HeapPageId currentPageId = null;
    private Iterator<Tuple> currentIterator = null;
    private Boolean open = false;
    private BufferPool bufferPool;

    public HeapFileIterator(TransactionId tid, int tableId, int maxPageNumber) {
        this.tableId = tableId;
        this.maxPageNumber = maxPageNumber;
        this.tid = tid;
        this.bufferPool = Database.getBufferPool();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        open = true;
        currentPageNumber = 0;
        currentPageId = new HeapPageId(tableId, currentPageNumber);
        currentPageNumber++;
        currentPage = (HeapPage) bufferPool.getPage(tid, currentPageId, READ_ONLY);
//        System.out.println("current open pid" + currentPage.getId().toString());

        if (currentPage == null) {
            System.out.println("open fail, no page");
            throw new DbException("open fail, no page");
        }
        currentIterator = currentPage.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open = true;
        currentPageNumber = 0;
        bufferPool.releasePage(tid, currentPage.pid, READ_ONLY);
        currentPageId = new HeapPageId(tableId, currentPageNumber);
        currentPageNumber++;
        currentPage = (HeapPage) bufferPool.getPage(tid, currentPageId, READ_ONLY);
        if (currentPage == null) {
            System.out.println("rewind fail, no page");
            throw new DbException("rewind fail, no page");
        }
        currentIterator = currentPage.iterator();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // not open
        if (!open) {
            throw new IllegalStateException();
        }
        if (currentIterator == null) {
            System.out.println("currentIterator is null");
            return null;
        }
        // if current page still has next
        if (currentIterator.hasNext()) {
            return currentIterator.next();
        }
        // current page do not have next
        // if it is the last page, then return null
        if (currentPageNumber >= maxPageNumber) {
            return null;
        }
        // then read new page
        bufferPool.releasePage(tid, currentPageId, READ_ONLY);
        currentPageId = new HeapPageId(this.tableId, currentPageNumber);
        currentPageNumber++;
        currentPage = (HeapPage) bufferPool.getPage(tid, currentPageId, READ_ONLY);
//        System.out.println("current open pid" + currentPage.getId().toString());
        if (currentPage == null) {
            // no new page
            currentIterator = null;
            System.out.printf("readNext fail, currentPageNumber %d has no page\n", currentPageNumber - 1);
            return null;
        }
        currentIterator = currentPage.iterator();
        if (currentIterator.hasNext()) {
            return currentIterator.next();
        }
        // currentPage is an empty page
        // read next page
        // return null;
        return readNext();
    }

    @Override
    public void close() {
        super.close();
        open = false;
        bufferPool.releasePage(tid, currentPageId, READ_ONLY);
    }
}
