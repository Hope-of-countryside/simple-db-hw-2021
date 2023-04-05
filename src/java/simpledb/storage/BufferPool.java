package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.util.LRU;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static simpledb.common.Permissions.READ_ONLY;
import static simpledb.common.Permissions.READ_WRITE;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    public static int maxPages = DEFAULT_PAGES;

    private LRU<PageId, Page> pages;

    private LockManager lockManager;

//    private HashMap<Integer, ArrayList<Page>> tableIdToDirtyAndNotExistPages;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        maxPages = numPages;
        pages = new LRU<>(maxPages);
        lockManager = new LockManager();
//        tableIdToDirtyAndNotExistPages = new HashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        lockManager.getLock(tid, pid, perm);
        if (pages.containsKey(pid)) {
//          System.out.println("page" + pid + " is in buffer pool");
            return pages.get(pid);
        } else {
            Page p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pages.put(pid, p, page -> page.isDirty() == null);
            System.out.println("pages " + pages.toString());
            return p;
        }

    }

//    public void addPage(Integer tableId, Page p, PageId pid) throws DbException {
//        this.pages.put(pid, p, page -> page.isDirty() == null);
//    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        try {
            lockManager.unsafeReleaseLock(tid, pid);
        } catch (Exception e) {
            System.out.println("unsafeReleasePage failed: " + e);
        }
    }

    public void releasePage(TransactionId tid, PageId pid, Permissions perm) {
        try {
            lockManager.releaseLock(tid, pid, perm);
        } catch (Exception e) {
            System.out.println("releasePage failed: " + e);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
//        return lockManager.holdsLock(tid, pid, null);
        return true;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        List<PageId> pids = lockManager.getPageIdsHoldByAndAndTransactionId(tid);

        for (PageId pid : pids) {
            try {
                Page page = getPage(tid, pid, READ_ONLY);
                if (page.isDirty() == tid) {
                    if (commit) {
                        // flush change for dirty pages
                        flushPage(pid);
                    } else {
                        // revert change for dirty pages
                        refreshPageFromDisk(pid);
                    }
                }
                // release lock
                lockManager.unsafeReleaseLock(tid, pid);
            } catch (Exception e) {
                System.out.println("transactionComplete failed: " + e);
            }
        }
    }

    public void refreshPageFromDisk(PageId pid) {
        pages.remove(pid);
        Page p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        try {
            pages.put(pid, p, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        dbFile.insertTuple(tid, t);
//        List<Page> insertResult = dbFile.insertTuple(tid, t);
//        for (Page p : insertResult) {
//            System.out.println("inserted pid: " + p.getId().toString());
//            p.markDirty(true, tid);
//            addPage(p, p.getId());
//        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        //System.out.println("database file: " + dbFile.toString());
        List<Page> deleteResult = dbFile.deleteTuple(tid, t);
        for (Page p : deleteResult) {
            p.markDirty(true, tid);
//            flushPage(p.getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Iterator<Page> it = pages.iterator3(); it.hasNext(); ) {
            Page page = it.next();
            flushPage(page.getId());
            page.markDirty(false, null);
//            lockManager.refreshPageLock(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pages.remove(pid);
        lockManager.refreshPageLock(pid);

    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = this.pages.get(pid);
        if (page == null) {
            throw new IOException("this page is not in memory");
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(page);
        page.markDirty(false, null);
//        lockManager.refreshPageLock(pid);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        try {
            Iterator<PageId> iterator = pages.iterator();
            while (iterator.hasNext()) {
                PageId pageId = iterator.next();
                if (pages.get(pageId).isDirty() == null) {
                    flushPage(pageId);
                    lockManager.refreshPageLock(pageId);
                    pages.remove(pageId);
                }
            }
            throw new DbException("no clean pages");
        } catch (IOException e) {
            throw new DbException(e.getMessage());
        }
    }

//    public HashMap<Integer, ArrayList<Page>> getTableIdToDirtyAndNotExistPages(){
//        return tableIdToDirtyAndNotExistPages;
//    }

    class LockManager {
        // transaction
        // page
        // perm
        private ConcurrentHashMap<PageId, Object> locksForPage;
        private HashMap<PageId, LockNode> pageIdToLockNode;

        private DeadLockDetector deadLockDetector;

        public LockManager() {
            locksForPage = new ConcurrentHashMap<>();
            pageIdToLockNode = new HashMap<>();
            deadLockDetector = new DeadLockDetector();
        }

        public void getLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
            System.out.println("pageIdToLockNode: " + pageIdToLockNode);
//            System.out.println("get lock tid " + tid + " pid " + pid + " perm " + perm);
            locksForPage.computeIfAbsent(pid, k -> new Object());
            synchronized (locksForPage.get(pid)) {
                LockNode lockNode = pageIdToLockNode.get(pid);
                if (lockNode == null) {
                    pageIdToLockNode.put(pid, new LockNode(perm, tid, this.deadLockDetector, pid));
                    return;
                }
                lockNode.getLock(tid, perm);
                deadLockDetector.pageAllocatedToTransaction(tid, pid);
            }
        }

        public void releaseLock(TransactionId tid, PageId pid, Permissions perm) throws DbException {
//            System.out.println("release lock tid " + tid + " pid " + pid);
            LockNode lockNode = pageIdToLockNode.get(pid);
            if (lockNode == null) {
                throw new DbException("no lock has been get, tid " + tid.getId() + " pid " + pid);
            }
            lockNode.releaseLock(tid, perm);
            deadLockDetector.removePageAllocatedToTransaction(tid, pid);
        }

        public void unsafeReleaseLock(TransactionId tid, PageId pid) throws DbException {
//            System.out.println("unsafe release lock tid " + tid + " pid " + pid);
            LockNode lockNode = pageIdToLockNode.get(pid);
            if (lockNode == null) {
                throw new DbException("no lock has been get, tid " + tid.getId() + " pid " + pid);
            }
            lockNode.unsafeReleaseLock(tid);
            deadLockDetector.removeByTransactionId(tid);
        }

        public void refreshPageLock(PageId pid) {
            synchronized (locksForPage.get(pid)) {
                pageIdToLockNode.remove(pid);
            }
        }

        public List<PageId> getPageIdsHoldByAndAndTransactionId(TransactionId tid) {
            ArrayList<PageId> pids = new ArrayList<>();
            for (Map.Entry<PageId, LockNode> entry : pageIdToLockNode.entrySet()) {
                if (entry.getValue().holdsLock(tid)) {
                    pids.add(entry.getKey());
                }
            }
            return pids;
        }

        class LockNode {
            PageId pageId;
            private ConcurrentHashMap<Permissions, ConcurrentHashMap<TransactionId, Integer>> permToTidAndTimes; // perm may be requested many times
            private ArrayBlockingQueue<Object> signal; // signal to get the permission
            private DeadLockDetector deadLockDetector;

            public LockNode(Permissions perm, TransactionId tid, DeadLockDetector deadLockDetector, PageId pageId) {
                this.deadLockDetector = deadLockDetector;
                this.pageId = pageId;
                this.permToTidAndTimes = new ConcurrentHashMap<>();
                changeLockOwnership(perm, tid);
            }

            public synchronized void getLock(TransactionId tid, Permissions newPerm) throws TransactionAbortedException {
                // empty
                if (permToTidAndTimes.isEmpty()) {
                    changeLockOwnership(newPerm, tid);
                    return;
                }
                // already has this perm, times++
                if (permToTidAndTimes.containsKey(newPerm) && permToTidAndTimes.get(newPerm).containsKey(tid)) {
                    permToTidAndTimes.get(newPerm).computeIfPresent(tid, (k, v) -> v + 1);
                    return;
                }
                if (newPerm == Permissions.READ_ONLY) {
                    // shared lock
                    if (!permToTidAndTimes.containsKey(READ_WRITE) && permToTidAndTimes.containsKey(READ_ONLY)) {
                        permToTidAndTimes.get(READ_ONLY).compute(tid, (k, v) -> {
                            if (v == null) v = 1;
                            else v += 1;
                            return v;
                        });
                        return;
                    }
                    // down grade
                    if (permToTidAndTimes.containsKey(READ_WRITE) &&
                            permToTidAndTimes.get(READ_WRITE).containsKey(tid) &&
                            permToTidAndTimes.get(READ_WRITE).size() == 1) {
                        permToTidAndTimes.put(newPerm, new ConcurrentHashMap<TransactionId, Integer>() {{
                            this.put(tid, 1);
                        }});
                        return;
                    }
                } else if (newPerm == READ_WRITE) {
                    if (permToTidAndTimes.containsKey(READ_ONLY) &&
                            permToTidAndTimes.get(READ_ONLY).containsKey(tid) &&
                            permToTidAndTimes.get(READ_ONLY).size() == 1) {
                        permToTidAndTimes.put(newPerm, new ConcurrentHashMap<TransactionId, Integer>() {{
                            this.put(tid, 1);
                        }});
                        return;
                    }
                }

                try {
                    Thread.sleep(new Random().nextInt(50));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                throw new TransactionAbortedException();
//                waitAllLockReleased(tid);
//                changeLockOwnership(newPerm, tid);
            }

            public void releaseLock(TransactionId tid, Permissions perm) throws DbException {
                if (!permToTidAndTimes.containsKey(perm) || !permToTidAndTimes.get(perm).containsKey(tid)) {
                    return;
//                    throw new DbException("transaction " + tid.getId() + " do not has perm " + perm);
                }
                if (permToTidAndTimes.get(perm).get(tid) == 1) {
                    permToTidAndTimes.get(perm).remove(tid);
                } else {
                    permToTidAndTimes.get(perm).computeIfPresent(tid, (k, v) -> v - 1);
                }
                if (permToTidAndTimes.get(perm).size() == 0) {
                    permToTidAndTimes.remove(perm);
                }
                if (permToTidAndTimes.isEmpty()) {
                    try {
                        signal.put(new Object());
                    } catch (Exception e) {
                        System.out.println("releaseLock failed " + e);
                    }
                }
            }

            public void unsafeReleaseLock(TransactionId tid) {
                if (permToTidAndTimes.containsKey(READ_ONLY)) {
                    permToTidAndTimes.get(READ_ONLY).remove(tid);
                    if (permToTidAndTimes.get(READ_ONLY).size() == 0) {
                        permToTidAndTimes.remove(READ_ONLY);
                    }
                }
                if (permToTidAndTimes.containsKey(READ_WRITE)) {
                    permToTidAndTimes.get(READ_WRITE).remove(tid);
                    if (permToTidAndTimes.get(READ_WRITE).size() == 0) {
                        permToTidAndTimes.remove(READ_WRITE);
                    }
                }
                if (permToTidAndTimes.isEmpty()) {
                    try {
                        signal.put(new Object());
                    } catch (Exception e) {
                        System.out.println("releaseLock failed " + e);
                    }
                }
            }

            private void waitAllLockReleased(TransactionId tid) throws TransactionAbortedException {
                deadLockDetector.transactionWaitForPage(tid, pageId);
                System.out.println("tid " + tid + " wait for pageId" + pageId);
                try {
                    signal.take();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                deadLockDetector.removeTransactionWaitForPage(tid, pageId);
            }

            private void changeLockOwnership(Permissions perm, TransactionId tid) {
                permToTidAndTimes.clear();
                permToTidAndTimes.put(perm, new ConcurrentHashMap<TransactionId, Integer>() {{
                    this.put(tid, 1);
                }});
                signal = new ArrayBlockingQueue<>(1);
            }

            public boolean holdsLock(TransactionId tid) {
                return (permToTidAndTimes.containsKey(READ_ONLY) && permToTidAndTimes.get(READ_ONLY).containsKey(tid)) ||
                        (permToTidAndTimes.containsKey(READ_WRITE) && permToTidAndTimes.get(READ_WRITE).containsKey(tid));
            }

            @Override
            public String toString() {
                return permToTidAndTimes.toString();
            }
        }
    }


    class DeadLockDetector {
        private HashMap<TransactionId, ArrayList<PageId>> transactionIdsWaitForPages;
        private HashMap<PageId, ArrayList<TransactionId>> pagesAllocatedToTransactionIds;

        public DeadLockDetector() {
            transactionIdsWaitForPages = new HashMap<>();
            pagesAllocatedToTransactionIds = new HashMap<>();
        }

        synchronized public void removeByTransactionId(TransactionId tid) {
            transactionIdsWaitForPages.remove(tid);
            for (Map.Entry<PageId, ArrayList<TransactionId>> entry : pagesAllocatedToTransactionIds.entrySet()) {
                if (entry.getValue().contains(tid)) {
                    ArrayList<TransactionId> newArray = entry.getValue().stream().filter(transactionId -> transactionId != tid).collect(Collectors.toCollection(ArrayList::new));
                    pagesAllocatedToTransactionIds.put(entry.getKey(), newArray);
                }
            }
        }

        synchronized public void transactionWaitForPage(TransactionId tid, PageId pageId) throws TransactionAbortedException {
            transactionIdsWaitForPages.compute(tid, (k, v) -> {
                if (v == null) return new ArrayList<PageId>() {{
                    add(pageId);
                }};
                v.add(pageId);
                return v;
            });
            detectDeadLock(tid, null, new HashSet<TransactionId>());
        }

        synchronized public void removeTransactionWaitForPage(TransactionId tid, PageId pageId) {
            transactionIdsWaitForPages.compute(tid, (k, v) -> {
                if (v == null) {
                    throw new IllegalArgumentException("tid " + tid + " not exists");
                }
                if (v.size() == 1 && v.get(0).equals(pageId)) {
                    return null;
                }
                v.remove(pageId);
                return v;
            });
        }

        synchronized public void pageAllocatedToTransaction(TransactionId tid, PageId pageId) throws TransactionAbortedException {
            pagesAllocatedToTransactionIds.compute(pageId, (k, v) -> {
                if (v == null) return new ArrayList<TransactionId>() {{
                    add(tid);
                }};
                v.add(tid);
                return v;
            });
//            detectDeadLock(null, pageId, new HashSet<TransactionId>());
        }

        synchronized public void removePageAllocatedToTransaction(TransactionId tid, PageId pageId) {
            pagesAllocatedToTransactionIds.compute(pageId, (k, v) -> {
                if (v == null) {
                    throw new IllegalArgumentException("pageId " + pageId + " not exists");
                }
                if (v.size() == 1 && v.get(0).equals(tid)) {
                    return null;
                }
                v.remove(tid);
                return v;
            });
        }

        private void detectDeadLock(TransactionId tid, PageId pageId, HashSet<TransactionId> tidSet) throws TransactionAbortedException {
            if (tid != null) {
                if (tidSet.contains(tid)) {
                    System.out.println("tid " + tid + "\ntransactionIdsWaitForPages: " + transactionIdsWaitForPages + "\n pagesAllocatedToTransactionIds: " + pagesAllocatedToTransactionIds);
                    throw new TransactionAbortedException();
                }
                tidSet.add(tid);
                ArrayList<PageId> pageIds = transactionIdsWaitForPages.get(tid);
                if (pageIds != null) {
                    for (PageId temp : pageIds) {
                        detectDeadLock(null, temp, tidSet);
                    }
                }

            }

            if (pageId != null) {
                ArrayList<TransactionId> transactionIds = pagesAllocatedToTransactionIds.get(pageId);
                if (transactionIds != null) {
                    for (TransactionId temp : transactionIds) {
                        detectDeadLock(temp, null, tidSet);
                    }
                }
            }
        }
    }


}
