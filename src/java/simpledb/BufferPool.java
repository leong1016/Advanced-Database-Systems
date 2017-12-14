package simpledb;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

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
    private List<PageId> evictionList;

    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private static class LockManager {
        
        private static TransactionId NO_LOCK = new TransactionId();

        private ConcurrentHashMap<PageId, Object> locks;
        private ConcurrentHashMap<PageId, HashSet<TransactionId>> share;
        private ConcurrentHashMap<PageId, TransactionId> exclusive;
        private ConcurrentHashMap<TransactionId, HashSet<PageId>> reverse;
//        private ConcurrentHashMap<TransactionId, HashSet<PageId>> reverseShare;
//        private ConcurrentHashMap<TransactionId, HashSet<PageId>> reverseExclusive;

        public LockManager() {
            locks = new ConcurrentHashMap<>();
            share = new ConcurrentHashMap<>();
            exclusive = new ConcurrentHashMap<>();
            reverse = new ConcurrentHashMap<>();
//            reverseShare = new ConcurrentHashMap<>();
//            reverseExclusive = new ConcurrentHashMap<>();
        }

        public Object getLock(PageId pid) {
            if (!locks.containsKey(pid)) {
                locks.put(pid, new Object());
                share.put(pid, new HashSet<>());
                exclusive.put(pid, NO_LOCK);
            }
            return locks.get(pid);
        }
        
        public void acquireLock(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException {

            long start = System.currentTimeMillis();
            
            Object lock = getLock(pid);

            if (perm.equals(Permissions.READ_ONLY) && !share.get(pid).contains(tid)) {
                while (true) {
                    synchronized (lock) {
                        if (exclusive.get(pid).equals(NO_LOCK) || exclusive.get(pid).equals(tid)) {
                            synchronized (share.get(pid)) {
                                share.get(pid).add(tid);
                            }
                            break;
                        } else {
                            long now = System.currentTimeMillis();
                            if (now - start > 200) {
                                throw new TransactionAbortedException();
                            }
                        }
                    }
                }
//                    if (acquireShareLock(pid, tid)) {
//                        break;
//                    } else {
//                        long now = System.currentTimeMillis();
//                        if (now - start > 200) {
//                            throw new TransactionAbortedException();
//                        }
//                    }
//                    try {
//                        Thread.sleep(1);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
            } else if (perm.equals(Permissions.READ_WRITE) && !exclusive.get(pid).equals(tid)) {
                while (true) {
                    synchronized (lock) {
                        if ((share.get(pid).size() == 0 || share.get(pid).size() == 1 && share.get(pid).contains(tid)) 
                                && exclusive.get(pid).equals(NO_LOCK)) {
                            synchronized (share.get(pid)) {
                                share.get(pid).remove(tid);
                            }
                            synchronized (exclusive.get(pid)) {
                                exclusive.put(pid, tid);
                            }
                            break;
                        } else {
                            long now = System.currentTimeMillis();
                            if (now - start > 200) {
                                throw new TransactionAbortedException();
                            }
                        }
                    }
                    
//                    if (acquireExclusiveLock(pid, tid)) {
//                        break;
//                    } else {
//                        long now = System.currentTimeMillis();
//                        if (now - start > 200) {
//                            throw new TransactionAbortedException();
//                        }
//                    }
//                    try {
//                        Thread.sleep(1);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }
            }
            
            if (!(this.reverse.containsKey(tid))) {
                this.reverse.put(tid, new HashSet<PageId>());
            }
            
            synchronized(this.reverse.get(tid)) {
                this.reverse.get(tid).add(pid);
            }
        }
        
        public void releaseLock(TransactionId tid, PageId pid) {
            if (!reverse.containsKey(tid))
                return;
            
            Object lock = this.getLock(pid);
            synchronized (lock) {
                if (exclusive.get(pid).equals(tid)) {
                    exclusive.put(pid, NO_LOCK);
                }
                
                synchronized (share.get(pid)) {
                    share.get(pid).remove(tid);
                }
            }
            
            synchronized(this.reverse.get(tid)) {
                this.reverse.get(tid).remove(pid);
            }
        }
        
        public void releaseAllLock(TransactionId tid) {
            if (!reverse.containsKey(tid))
                return;
            
            HashSet<PageId> pids = reverse.get(tid);
            for (PageId pid : pids) {
                Object lock = getLock(pid);
                synchronized (lock) {
                    if (exclusive.get(pid).equals(tid)) {
                        exclusive.put(pid, NO_LOCK);
                    }
                    synchronized (share.get(pid)) {
                        share.get(pid).remove(tid);
                    }
                }
            }
            
            reverse.remove(tid);
        }
        
        
        
        
        
        
        
        
        
        

//        public boolean acquireShareLock(PageId pid, TransactionId tid) {
//
//            if (reverseShare.containsKey(tid) && reverseShare.get(tid).contains(pid)) {
//                System.out.println(tid.getId()+"has already share locked:"+pid.pageNumber());
//                return true;
//            }
//            if (reverseExclusive.containsKey(tid) && reverseExclusive.get(tid).contains(pid)) {
//                System.out.println(tid.getId()+"has already share locked:"+pid.pageNumber());
//                return true;
//            }
//
//            if (exclusive.containsKey(pid)) {
//                System.out.println(tid.getId()+"failed to share lock:"+pid.pageNumber());
//                return false;
//            } else {
//                if (share.containsKey(pid)) {
//                    HashSet<TransactionId> tids = share.get(pid);
//                    tids.add(tid);
//                    share.put(pid, tids);
//                } else {
//                    HashSet<TransactionId> tids = new HashSet<>();
//                    tids.add(tid);
//                    share.put(pid, tids);
//                }
//                if (reverseShare.containsKey(tid)) {
//                    HashSet<PageId> pids = reverseShare.get(tid);
//                    pids.add(pid);
//                    reverseShare.put(tid, pids);
//                } else {
//                    HashSet<PageId> pids = new HashSet<>();
//                    pids.add(pid);
//                    reverseShare.put(tid, pids);
//                }
//                System.out.println(tid.getId()+"successfully share locked:"+pid.pageNumber());
//                return true;
//            }
//        }
//
//        public boolean acquireExclusiveLock(PageId pid, TransactionId tid) {
//            if (reverseExclusive.containsKey(tid) && reverseExclusive.get(tid).contains(pid)) {
//                System.out.println(tid.getId()+"has already exclusive locked:"+pid.pageNumber());
//                return true;
//            }
//            if (exclusive.containsKey(pid)) {
//                System.out.println(tid.getId()+"failed to exclusive lock:"+pid.pageNumber());
//                return false;
//            } else if (share.containsKey(pid) && (share.get(pid).size() > 1 || !share.get(pid).contains(tid))) {
//                System.out.println(tid.getId()+"failed to exclusive lock:"+pid.pageNumber());
////                for (TransactionId t : share.get(pid)) {
////                    System.out.println(t.getId());
////                }
//                return false;
//            } else {
//                if (reverseShare.containsKey(tid) && reverseShare.get(tid).contains(pid)) {
//                    HashSet<TransactionId> tids = share.get(pid);
//                    tids.remove(tid);
//                    if (tids.size() == 0) {
//                        share.remove(pid);
//                    } else {
//                        share.put(pid, tids);
//                    }
//                    HashSet<PageId> pids = reverseShare.get(tid);
//                    pids.remove(pid);
//                    if (pids.size() == 0) {
//                        reverseShare.remove(tid);
//                    } else {
//                        reverseShare.put(tid, pids);
//                    }
//                }
//                exclusive.put(pid, tid);
//                if (reverseExclusive.containsKey(tid)) {
//                    HashSet<PageId> pids = reverseExclusive.get(tid);
//                    pids.add(pid);
//                    reverseExclusive.put(tid, pids);
//                } else {
//                    HashSet<PageId> pids = new HashSet<>();
//                    pids.add(pid);
//                    reverseExclusive.put(tid, pids);
//                }
//                System.out.println(tid.getId()+"successfully exclusive locked:"+pid.pageNumber());
//                return true;
//            }
//        }
//
//        public void releaseLock(PageId pid, TransactionId tid) {
//            if (share.containsKey(pid)) {
//                HashSet<TransactionId> tids = share.get(pid);
//                tids.remove(tid);
//                if (tids.size() == 0) {
//                    share.remove(pid);
//                } else {
//                    share.put(pid, tids);
//                }
//            }
//            if (exclusive.containsKey(pid)) {
//                exclusive.remove(pid);
//            }
//            if (reverseShare.containsKey(tid)) {
//                HashSet<PageId> pids = reverseShare.get(tid);
//                pids.remove(pid);
//                if (pids.size() == 0) {
//                    reverseShare.remove(tid);
//                } else {
//                    reverseShare.put(tid, pids);
//                }
//            }
//            if (reverseExclusive.containsKey(tid)) {
//                HashSet<PageId> pids = reverseExclusive.get(tid);
//                pids.remove(pid);
//                if (pids.size() == 0) {
//                    reverseExclusive.remove(tid);
//                } else {
//                    reverseExclusive.put(tid, pids);
//                }
//            }
//        }
//
//        public void releaseAllLock(TransactionId tid) {
//            System.out.println(tid.getId()+"release all locks");
//            if (reverseShare.containsKey(tid)) {
//                HashSet<PageId> pids = reverseShare.get(tid);
//                for (PageId pid : pids) {
//                    if (share.containsKey(pid)) {
//                        HashSet<TransactionId> tids = share.get(pid);
//                        tids.remove(tid);
//                        if (tids.size() == 0) {
//                            share.remove(pid);
//                        } else {
//                            share.put(pid, tids);
//                        }
//                    }
//                }
//                reverseShare.remove(tid);
//            }
//            if (reverseExclusive.containsKey(tid)) {
//                HashSet<PageId> pids = reverseExclusive.get(tid);
//                for (PageId pid : pids) {
//                    if (exclusive.containsKey(pid)) {
//                        exclusive.remove(pid);
//                    }
//                }
//                reverseExclusive.remove(tid);
//            }
//        }

        public boolean holdsLock(TransactionId tid, PageId pid) {
            if (!(this.reverse.containsKey(tid))) {
                return false;
            }

            synchronized(this.reverse.get(tid)) {
                return this.reverse.get(tid).contains(pid);
            }
        }

        public HashSet<PageId> exclusivePages(TransactionId tid) {
            if (reverse.containsKey(tid)) {
                return reverse.get(tid);
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
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
//        manager.acquireLock(pid, tid, perm);
        //        locker.acquireLock(tid, pid, perm);
        if (pool.containsKey(pid)) {
            evictionList.remove(pid);
            evictionList.add(pid);
            manager.acquireLock(pid, tid, perm);
            return pool.get(pid);
        } else {
            if (pool.size() == maxPages) {
                evictPage(tid);
            }
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            pool.put(pid, page);
            evictionList.add(pid);
            manager.acquireLock(pid, tid, perm);
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
        manager.releaseLock(tid, pid);
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
    public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
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
    public synchronized void deleteTuple(TransactionId tid, Tuple t)
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
    private synchronized  void evictPage(TransactionId tid) throws DbException {
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
                    releasePage(tid, pid);
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
