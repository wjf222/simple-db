package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

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

    private final File file;
    private final TupleDesc td;
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
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
        return this.file.getAbsoluteFile().hashCode();
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
        if(pid.getPageNumber() >= this.numPages() || pid.getPageNumber() < 0) {
            return null;
        }
        return readHeapPage(pid);
    }
    private HeapPage readHeapPage(PageId pid){
        try {
            InputStream is = new FileInputStream(file);

            byte[] buf = new byte[BufferPool.getPageSize()];
            int offset = pid.getPageNumber()*BufferPool.getPageSize();
            is.skip(offset);
            int count = is.read(buf);
            if(count < BufferPool.getPageSize()) {
                return null;
            }
            is.close();
            return new HeapPage(new HeapPageId(getId(),pid.getPageNumber()),buf);
        } catch (IOException e) {
            e.printStackTrace();
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
        return (int)file.length()/BufferPool.getPageSize();
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
        return new DbFileIterator() {
            int cursor = -1;
            Iterator<Tuple> itr;
            final int pages = numPages();
            private Iterator<Tuple> readNextPage() throws TransactionAbortedException, DbException {
                cursor++;
                if(cursor < pages){
                    HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(getId(),cursor),Permissions.READ_ONLY);
                    return p.iterator();
                }
                return null;
            }
            @Override
            public void open() throws DbException, TransactionAbortedException {
                itr = readNextPage();
                if(itr == null){
                    throw new DbException("Can not open dbfile");
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(cursor < 0) return false;
                if(itr == null) {
                    return false;
                }
                while(!itr.hasNext()){
                    itr = readNextPage();
                    if(itr == null){
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(hasNext()) {
                    return itr.next();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                cursor = -1;
                itr = null;
                itr = readNextPage();
                if(itr == null){
                    throw new DbException("Can not open dbfile");
                }
            }

            @Override
            public void close() {
                cursor = -1;
                itr = null;
            }
        };
    }
}

