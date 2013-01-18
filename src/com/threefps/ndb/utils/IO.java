/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * IO utility class
 * @author sluu
 */
public class IO {
    /**
     * Read N bytes from a channel
     * @param f
     * @param n
     * @return 
     */
    public static ByteBuffer read(FileChannel f, int n) throws IOException {
        ByteBuffer buff = ByteBuffer.allocate(n);
        int nread = 0;
        do {
            nread = f.read(buff);
        } while (nread != -1 && buff.hasRemaining());
        return buff;
    }
    
    /**
     * Write the buffer to a file channel
     * @param f
     * @param b
     * @param force Should the file channel force flushing?
     */
    public static void write(FileChannel f, ByteBuffer b, boolean force) throws IOException {
        while (b.hasRemaining())
            f.write(b);
        if (force)
            f.force(true);
    }
}
