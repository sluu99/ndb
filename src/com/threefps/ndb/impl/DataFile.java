/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import com.threefps.ndb.errors.DataException;
import java.io.Closeable;
import java.io.IOException;
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

    /**
     * Page size (4 megabytes)
     */
    public static final int PAGE_SIZE = 1024 * 1024 * 4;
    /**
     * Header size (1024 bytes)
     */
    private static final int HEADER_SIZE = 1024;
    /**
     * The header reserves 8 bytes to indicate where new data should be appended.
     * This is the location of those 8 bytes in the header.
     */
    private static final int APPEND_FLAG_POS = 4;

    /**
     * Calculates page number from position
     *
     * @param pos The absolute position (does not include file header offset)
     * @return The zero-based page number
     */
    private static int page(long pos) {
        return (int) (pos / PAGE_SIZE);
    }

    /**
     * Calculate the position relative to a page
     *
     * @param pos The absolute position (does not include file header offset)
     * @return The position in relative to a page
     */
    private static int ppos(long pos) {
        return (int) (pos % PAGE_SIZE);
    }
    
    /**
     * The file channel
     */
    private FileChannel ch = null;
    /**
     * Buffer pages
     */
    private final ArrayList<MappedByteBuffer> pages = new ArrayList<>();
    /**
     * Header page
     */
    MappedByteBuffer headerPage = null;
    /**
     * number of bytes mapped (does not include header size)
     */
    private long bytesMapped = 0;
    /**
     * the position for appending new data
     */
    private long appendAt = 0;

    /**
     * Open a file in READ and WRITE mode at that path
     *
     * @param path Path of the file
     * @throws IOException If an I/O error occurred
     */
    public DataFile(Path path) throws IOException {
        ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        map();
        readHeader();
    }

    /**
     * Map the file into memory pages
     *
     * @throws IOException If an I/O error occurred
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
     *
     * @throws IOException If an I/O error occurred
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
     * Write to the buffer.
     *
     * @param pos Position of the file to write to. (This is the position before
     * calculating the file header offset.
     * @param src The data to be written
     * @param offset Position of the first byte to be written from 'src'
     * @param length Number of bytes to be written
     * @throws IOException If some I/O error occurred
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
     * Append new data to the file. Certain part of the file will be locked in
     * order to make sure no other thread is appending data to the same
     * location.
     *
     * @param b The binary data to be appended
     * @param offset The position to start reading data from the byte array
     * @param length Number of bytes to write, must not exceed PAGE_SIZE
     * @return Location of the file where data was appended to
     * @throws DataException If length is greater than PAGE_SIZE
     * @throws IOException If an I/O error occurred
     */
    public long append(byte[] b, int offset, int length) throws DataException, IOException {
        if (length > PAGE_SIZE) {
            throw new DataException("length must not be greater than PAGE_SIZE (" + PAGE_SIZE + ")");
        }

        FileLock lock = null;
        try {
            lock = ch.lock(APPEND_FLAG_POS, 8, false); // make sure nobody else is appending data

            if ((appendAt + length) > bytesMapped) {
                // the data does not fit into current page
                // create a new pagel
                mapNextPage();
                // start writing at the begining of next page
                appendAt = ((long) pages.size() - 1L) * (long) PAGE_SIZE;
            }

            MappedByteBuffer p = pages.get(page(appendAt));
            p.position(ppos(appendAt));
            p.put(b, offset, length); // ignore the ByteBuffer that returns

            // update position
            appendAt += length;
            headerPage.putLong(APPEND_FLAG_POS, appendAt); // update append at flag

        } finally {
            if (lock != null) {
                lock.release();
            }
        }

        return (appendAt - length);
    }

    /**
     * Close the data file and the channel associated with it. It also hints the
     * channel to flush the bufferred data.
     *
     * @throws IOException If an I/O error occurred
     */
    @Override
    public void close() throws IOException {
        if (ch != null && ch.isOpen()) {
            ch.force(true);
            ch.close();
        }
    }
}
