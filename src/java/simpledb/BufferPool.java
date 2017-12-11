package simpledb;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    private int maxPages;
    private HashMap<PageId, Page> pool;
    private LockManager manager;
    private Locker locker;
    private List<PageId> evictionList;

    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private class LockManager {

        private ConcurrentHashMap<PageId, HashSet<TransactionId>> share;
        private ConcurrentHashMap<PageId, TransactionId> exclusive;
        private ConcurrentHashMap<TransactionId, HashSet<PageId>> reverseShare;
        private ConcurrentHashMap<TransactionId, HashSet<PageId>> reverseExclusive;

        public LockManager() {
            share = new ConcurrentHashMap<>();
            exclusive = new ConcurrentHashMap<>();
            reverseShare = new ConcurrentHashMap<>();
            reverseExclusive = new ConcurrentHashMap<>();
        }

        public void acquireLock(PageId pid, TransactionId tid, Permissions perm) {
            boolean isSuccess = false;
            if (perm.equals(Permissions.READ_ONLY)) {
                isSuccess = acquireShareLock(pid, tid);
            } else if (perm.equals(Permissions.READ_WRITE)) {
                isSuccess = acquireExclusiveLock(pid, tid);
            }
            while (!isSuccess) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (perm.equals(Permissions.READ_ONLY)) {
                    isSuccess = acquireShareLock(pid, tid);
                } else if (perm.equals(Permissions.READ_WRITE)) {
                    isSuccess = acquireExclusiveLock(pid, tid);
                }
            }
        }

        public boolean acquireShareLock(PageId pid, TransactionId tid) {
            if (reverseShare.containsKey(tid) && reverseShare.get(tid).contains(pid)) {
                return true;
            }
            if (reverseExclusive.containsKey(tid) && reverseExclusive.get(tid).contains(pid)) {
                return true;
            }

            if (exclusive.containsKey(pid)) {
                return false;
            } else {
                if (share.containsKey(pid)) {
                    HashSet<TransactionId> tids = share.get(pid);
                    tids.add(tid);
                    share.put(pid, tids);
                } else {
                    HashSet<TransactionId> tids = new HashSet<>();
                    tids.add(tid);
                    share.put(pid, tids);
                }
                if (reverseShare.containsKey(tid)) {
                    HashSet<PageId> pids = reverseShare.get(tid);
                    pids.add(pid);
                    reverseShare.put(tid, pids);
                } else {
                    HashSet<PageId> pids = new HashSet<>();
                    pids.add(pid);
                    reverseShare.put(tid, pids);
                }
                return true;
            }
        }

        public boolean acquireExclusiveLock(PageId pid, TransactionId tid) {
            if (reverseExclusive.containsKey(tid) && reverseExclusive.get(tid).contains(pid)) {
                return true;
            }
            if (reverseShare.containsKey(tid) && reverseShare.get(tid).contains(pid)) {
                HashSet<TransactionId> tids = share.get(pid);
                tids.remove(tid);
                if (tids.size() == 0) {
                    share.remove(pid);
                } else {
                    share.put(pid, tids);
                }
                HashSet<PageId> pids = reverseShare.get(tid);
                pids.remove(pid);
                if (pids.size() == 0) {
                    reverseShare.remove(tid);
                } else {
                    reverseShare.put(tid, pids);
                }
            }

            if (share.containsKey(pid) || exclusive.containsKey(pid)) {
                return false;
            } else {
                exclusive.put(pid, tid);
                if (reverseExclusive.containsKey(tid)) {
                    HashSet<PageId> pids = reverseExclusive.get(tid);
                    pids.add(pid);
                    reverseExclusive.put(tid, pids);
                } else {
                    HashSet<PageId> pids = new HashSet<>();
                    pids.add(pid);
                    reverseExclusive.put(tid, pids);
                }
                return true;
            }
        }

        public void releaseLock(PageId pid, TransactionId tid) {
            if (share.containsKey(pid)) {
                HashSet<TransactionId> tids = share.get(pid);
                tids.remove(tid);
                if (tids.size() == 0) {
                    share.remove(pid);
                } else {
                    share.put(pid, tids);
                }
            }
            if (exclusive.containsKey(pid)) {
                exclusive.remove(pid);
            }
            if (reverseShare.containsKey(tid)) {
                HashSet<PageId> pids = reverseShare.get(tid);
                pids.remove(pid);
                if (pids.size() == 0) {
                    reverseShare.remove(tid);
                } else {
                    reverseShare.put(tid, pids);
                }
            }
            if (reverseExclusive.containsKey(tid)) {
                HashSet<PageId> pids = reverseExclusive.get(tid);
                pids.remove(pid);
                if (pids.size() == 0) {
                    reverseExclusive.remove(tid);
                } else {
                    reverseExclusive.put(tid, pids);
                }
            }
        }

        public void releaseAllLock(TransactionId tid) {
            if (reverseShare.containsKey(tid)) {
                HashSet<PageId> pids = reverseShare.get(tid);
                for (PageId pid : pids) {
                    if (share.containsKey(pid)) {
                        HashSet<TransactionId> tids = share.get(pid);
                        tids.remove(tid);
                        if (tids.size() == 0) {
                            share.remove(pid);
                        } else {
                            share.put(pid, tids);
                        }
                    }
                }
                reverseShare.remove(tid);
            }
            if (reverseExclusive.containsKey(tid)) {
                HashSet<PageId> pids = reverseExclusive.get(tid);
                for (PageId pid : pids) {
                    if (exclusive.containsKey(pid)) {
                        exclusive.remove(pid);
                    }
                }
                reverseExclusive.remove(tid);
            }
        }

        public boolean holdsLock(TransactionId tid, PageId pid) {
            if (reverseShare.containsKey(tid) && reverseShare.get(tid).contains(pid)) {
                return true;
            }
            if (reverseExclusive.containsKey(tid) && reverseExclusive.get(tid).contains(pid)) {
                return true;
            }
            return false;
        }
        
        public HashSet<PageId> exclusivePages(TransactionId tid) {
            if (reverseExclusive.containsKey(tid)) {
                return reverseExclusive.get(tid);
            } else {
                return null;
            }
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        maxPages = numPages;
        pool = new HashMap<>(maxPages);
        manager = new LockManager();
        locker = new Locker();
        evictionList = new LinkedList<>();
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
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        manager.acquireLock(pid, tid, perm);
        //        locker.acquireLock(tid, pid, perm);
        if (pool.containsKey(pid)) {
            evictionList.remove(pid);
            evictionList.add(pid);
            return pool.get(pid);
        } else {
            if (pool.size() == maxPages) {
                evictPage();
            }
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            pool.put(pid, page);
            evictionList.add(pid);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        manager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return manager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            flushPages(tid);
        } else {
            discardPages(tid);
        }
        manager.releaseAllLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.insertTuple(tid, t);
        for (Page page : pages) {
            //            getPage(tid, page.getId(), Permissions.READ_WRITE);
            page.markDirty(true, tid);
            pool.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableid = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        List<Page> pages = file.deleteTuple(tid, t);
        for (Page page : pages) {
            //			getPage(tid, page.getId(), Permissions.READ_WRITE);
            page.markDirty(true, tid);
            pool.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page page : pool.values()) {
            if (page.isDirty() != null) {
                flushPage(page.getId());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pool.remove(pid);
    }
    
    public synchronized void discardPages(TransactionId tid) {
        HashSet<PageId> pids = manager.exclusivePages(tid);
        System.out.println(pids.toString());
        if (pids == null)
            return;
        for (PageId pid : pids) {
            System.out.println(pool.get(pid).toString());
            if (pool.get(pid).isDirty() != null) {
                discardPage(pid);
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pool.get(pid);
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        HashSet<PageId> pids = manager.exclusivePages(tid);
        if (pids == null)
            return;
        for (PageId pid : pids) {
            if (pool.get(pid).isDirty() != null) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        for (int i = 0; i < evictionList.size(); i++) {
            PageId pid = evictionList.get(i);
            if (!pool.containsKey(pid)) {
                evictionList.remove(i);
            } else if (pool.get(pid).isDirty() == null) {
                try {
                    flushPage(pid);
                    discardPage(pid);
                    evictionList.remove(i);
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        throw new DbException("All the pages are dirty!");

        //    		PageId pid = evictionList.get(0);
        //    		evictionList.remove(0);
        //        if (pool.get(pid).page.isDirty() != null) {
        //            try {
        //                flushPage(pid);
        //            } catch (IOException e) {
        //                e.printStackTrace();
        //            }
        //        }
        //        discardPage(pid);
    }

}
