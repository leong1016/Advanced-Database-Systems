package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private int tableid;
    private File file;
    private TupleDesc td;
    
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableid;
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            long start = pid.pageNumber() * BufferPool.getPageSize();
            byte[] data = new byte[BufferPool.getPageSize()];
//            raf.readFully(data, start, BufferPool.getPageSize());
            raf.seek(start);
            raf.readFully(data);
            raf.close();
            Page page = new HeapPage((HeapPageId) pid, data);
            return page;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        long start = page.getId().pageNumber() * BufferPool.getPageSize();
        byte[] data = page.getPageData();
        raf.seek(start);
        raf.write(data);
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int i;
        HeapPage page = null;
        ArrayList<Page> list = new ArrayList<>();
        for (i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(tableid, i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0)
                break;
        }
        if (i == numPages()) {
            HeapPageId pid = new HeapPageId(tableid, i);
            byte[] data = HeapPage.createEmptyPageData();
            HeapPage newpage = new HeapPage(pid, data);
            writePage(newpage);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            list.add(page);
        } else {
            page.insertTuple(t);
            list.add(page);
        }
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> list = new ArrayList<>();
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator2(tid);
    }
    
    private class HeapFileIterator implements DbFileIterator {

        TransactionId tid;
        int i;
        boolean isOpen;
        Iterator<Tuple> iterator;
        
        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        
        @Override
        public void open() throws DbException, TransactionAbortedException {
            i = 0;
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            iterator = page.iterator();
            isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen)
                return false;
            while (!iterator.hasNext()) {
                i++;
                if (i == numPages()) {
                    return false;
                } else {
                    PageId pid = new HeapPageId(getId(), i);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    iterator = page.iterator();
                }
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new NoSuchElementException();
            } else {
                return iterator.next();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }
    
    private class HeapFileIterator2 extends AbstractDbFileIterator {

        TransactionId tid;
        int i;
        boolean isOpen;
        Iterator<Tuple> iterator;
        
        public HeapFileIterator2(TransactionId tid) {
            this.tid = tid;
        }
        
        @Override
        public void open() throws DbException, TransactionAbortedException {
            i = 0;
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            iterator = page.iterator();
            isOpen = true;
        }
        
        @Override
        public void close() {
            super.close();
            isOpen = false;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return null;
            }
            while (!iterator.hasNext()) {
                i++;
                if (i == numPages()) {
                    return null;
                }
                PageId pid = new HeapPageId(getId(), i);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                iterator = page.iterator();
            }
            return iterator.next();
        }
    }
}

