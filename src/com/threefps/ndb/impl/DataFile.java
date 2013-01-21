/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import com.threefps.ndb.errors.DataException;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

/**
 * A paged memory mapped file. The first 1024 bytes of this file will be
 * overhead data.
 *
 * @author sluu
 */
public class DataFile implements Closeable {

    // constants
    public static final int PAGE_SIZE = 1024 * 1024 * 4; // 4 megabytes
    private static final int HEADER_SIZE = 1024; // 1024 bytes for header size
    private static final int APPEND_FLAG_POS = 4; // where is "append-position stored in the file"
    
    /**
     * Calculates page number from position
     * @param pos
     * @return 
     */
    private static final int page(long pos) {
        return (int) (pos / PAGE_SIZE);
    }
    
    /**
     * Calculate the position relative to a page
     * @param pos
     * @return 
     */
    private static final int ppos(long pos) {
        return (int) (pos % PAGE_SIZE);
    }
    
    // private fields
    private FileChannel ch = null;
    private final ArrayList<MappedByteBuffer> pages = new ArrayList<>();
    MappedByteBuffer headerPage = null;
    private long bytesMapped = 0; // number of bytes mapped (does not include header page)
    private long appendAt = 0; // the position for appending new data
    

    /**
     * Open a file in READ and WRITE mode at that path
     *
     * @param path
     */
    public DataFile(Path path) throws IOException {
        ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        map();
        readHeader();
    }

    /**
     * Map the file into memory pages
     */
    private void map() throws IOException {
        headerPage = ch.map(FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE);
        synchronized (pages) {
            long size = ch.size(); // the original file size
            bytesMapped = 0;
            do {
                pages.add(ch.map(FileChannel.MapMode.READ_WRITE, HEADER_SIZE + pages.size() * PAGE_SIZE, PAGE_SIZE));
                bytesMapped += PAGE_SIZE;
            } while (bytesMapped <= size);
        }
    }
    
    /**
     * Map the next page in the file (can potentially grow file).
     * @throws IOException 
     */
    private void mapNextPage() throws IOException {
        synchronized (pages) {
            pages.add(ch.map(FileChannel.MapMode.READ_WRITE, HEADER_SIZE + pages.size() * PAGE_SIZE, PAGE_SIZE));
            bytesMapped += PAGE_SIZE;
        }
    }
    
    /**
     * Read this file's header information
     */
    private void readHeader() {
        
        // the eight bytes from 4 to 11 indicates the next pre-position (position before head offset)
        // for appending new data
        appendAt = headerPage.getLong(APPEND_FLAG_POS);
    }

    /**
     * Read N bytes from the buffer into a destination starting at an offset
     *
     * @param pos Read from this position. Specify -1 to indicate the current
     * position
     * @param dst Destination of the data
     * @param offset Start saving data at this position in the byte array
     * @param length Number of bytes intended to read
     * @return Number of bytes read
     */
    public void read(long pos, byte[] dst, int offset, int length) {        
        MappedByteBuffer p = pages.get(page(pos));
        p.position(ppos(pos));
        p.get(dst, offset, length);
    }

    /**
     * Write to the buffer. force() is NOT called
     *
     * @param pos Write to this position.
     * position
     * @param b
     */
    public void write(long pos, byte[] src, int offset, int length) throws IOException {
                
        FileLock lock = null;
        try {
            lock = ch.lock(pos, length, false);
            MappedByteBuffer p = pages.get(page(pos));
            p.position(ppos(pos));
            p.put(src, offset, length);
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
    }
    
    /**
     * Append new data to the file
     * @param src
     * @param offset
     * @param length length must not exceed PAGE_SIZE
     * @throws DataException
     * @throws IOException 
     */
    public long append(byte[] src, int offset, int length) throws DataException, IOException {
        if (length > PAGE_SIZE)
            throw new DataException("length must not be larger than PAGE_SIZE (" + PAGE_SIZE + ")");
        
        FileLock lock = null;
        try {
            lock = ch.lock(APPEND_FLAG_POS, 8, false); // make sure nobody else is appending data
            
            if ((appendAt + length) > bytesMapped) {
                // the data does not fit into current page
                mapNextPage(); // create a new page
                appendAt = (pages.size() - 1) * PAGE_SIZE; // start writing at the begining of next page
            }
            
            MappedByteBuffer p = pages.get(page(appendAt));
            p.position(ppos(appendAt));
            p.put(src, offset, length);
            
            // update position
            appendAt += length;
            headerPage.putLong(APPEND_FLAG_POS, appendAt); // update append at flag
            
        } finally {
            if (lock != null) lock.release();
        }
        
        return (appendAt - length);
    }

    /**
     * Write n zero bytes to the channel
     *
     * @param pos Write to this position. Specify -1 to indicate the current
     * position
     * @param n
     * @throws IOException
     */
    public void writeZeros(long pos, int n) throws IOException {
        write(pos, new byte[n], 0, n);
    }

    @Override
    public void close() throws IOException {
        if (ch != null && ch.isOpen()) {
            ch.force(true);
            ch.close();
        }
    }
}
