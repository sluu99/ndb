/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * A wrapper for FileChannel that automatically flush after every 1024 bytes
 * @author sluu
 */
public class DataFile {
    
    private int count = 0; // number of bytes written since last flush
    private FileChannel f = null;
    private static final int LIMIT = 1024;
    
    public DataFile(FileChannel f) {
        this.f = f;
    }
    
    /**
     * Get the underlying channel
     * @return 
     */
    public FileChannel getChannel() {
        return f;
    }
    
    /**
     * Bump the number of bytes since last flush and perform a flush if
     * it's over the limit.
     * @param c 
     */
    public void add(int c) throws IOException {
        count += c;
        if (count > LIMIT) {
            f.force(false);
            count = 0;
        }            
    }
    
    /**
     * Read N bytes from a channel into a destination starting at an offset
     * 
     * @param pos Read from this position. Specify -1 to indicate the current position
     * @param dst Destination of the data
     * @param offset Start saving data at this position in the byte array
     * @param length Number of bytes intended to read
     * @return Number of bytes read
     */
    public int read(long pos, byte[] dst, int offset, int length) throws IOException {
        
        if (pos != -1) f.position(pos);            
        ByteBuffer buff = ByteBuffer.allocate(length);        
        int nread = 0, total = 0;
        do {
            nread = f.read(buff);
            total += nread;
        } while (nread != -1 && buff.hasRemaining());
        buff.rewind();
        buff.get(dst, offset, total);
        return total;
    }
    
    /**
     * Write the buffer to a file channel. force() is NOT called
     * @param pos Write to this position. Specify -1 to indicate the current position
     * @param b
     */
    public void write(long pos, byte[] src, int offset, int length) throws IOException {
        
        FileLock lock = null;
        try {
            lock = f.lock(pos, length, false);
            if (pos != -1) f.position(pos);
            ByteBuffer buff = ByteBuffer.allocate(length);
            buff.put(src, offset, length);
            buff.flip();
            while (buff.hasRemaining())
                f.write(buff);
        } finally {
            if (lock != null) lock.release();
        }
    }
    
    /**
     * Write n zero bytes to the channel
     * @param pos Write to this position. Specify -1 to indicate the current position
     * @param n
     * @throws IOException 
     */
    public void writeZeros(long pos, int n) throws IOException {
        write(pos, new byte[n], 0, n);
    }
}
