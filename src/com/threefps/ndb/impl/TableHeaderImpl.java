/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import com.threefps.ndb.Const;
import static com.threefps.ndb.Const.*;
import com.threefps.ndb.TableHeader;
import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.utils.B;
import java.io.IOException;
import static java.lang.System.arraycopy;
import java.util.Locale;

/**
 * Implementation of the table header
 * @author sluu
 */
public class TableHeaderImpl implements TableHeader {

    private static final int VERSION_SIZE = 1; // Size of the version field
    private static final int RECORD_COUNT_SIZE = 8; // Number of bytes
    
    private byte version = 0;
    private String name = null;
    private long recordCount = 0;
    private long newestRecordPos = 0;
    private long keyIndexRootPos = 0;
    
    // <editor-fold desc="Getters and Setters">

    public long getNewestRecordPos() {
        return newestRecordPos;
    }

    public long getKeyIndexRootPos() {
        return keyIndexRootPos;
    }
        
    @Override
    public byte getVersion() {
        return version;
    }

    @Override
    public long getRecordCount() {
        return recordCount;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Set the table version.
     * @param version The version number
     */
    public void setVersion(byte version) {
        this.version = version;
    }

    /**
     * Set the table name
     * @param name The table name
     */
    public void setName(String name) {
        this.name = name.trim().toLowerCase(Locale.getDefault());
    }

    /**
     * Set the number of records in the table
     * @param recordCount The number of records
     */
    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Set position of the newest record
     * @param newestRecord The record position
     */
    public void setNewestRecordPos(long newestRecord) {
        this.newestRecordPos = newestRecord;
    }

    /**
     * Set the position where the key-index tree root is
     * @param keyIndexRootPos Position of the key-index tree root
     */
    public void setKeyIndexRootPos(long keyIndexRootPos) {
        this.keyIndexRootPos = keyIndexRootPos;
    }
    // </editor-fold>
    
    /**
     * Increase the record count
     */
    public void incCount() {
        recordCount += 1;
    }
    
    /**
     * Load table header from file.
     * 
     * @param f The data file to read from.
     */
    public void read(DataFile f) {
        // read the fixed size data
        int len = VERSION_SIZE + RECORD_COUNT_SIZE + POINTER_SIZE + POINTER_SIZE + 1;        
        byte[] b = new byte[len];
        f.read(0, b, 0, len);
        
        int offset = 0;
        setVersion(b[0]); offset += VERSION_SIZE;
        setRecordCount(B.toLong(b, offset)); offset += RECORD_COUNT_SIZE;
        setNewestRecordPos(B.toLong(b, offset)); offset += POINTER_SIZE;
        setKeyIndexRootPos(B.toLong(b, offset)); offset += POINTER_SIZE;
        
        // read the table name
        byte tableNameLen = b[offset]; offset += 1;
        b = new byte[tableNameLen];
        f.read(offset, b, 0, tableNameLen);
        setName(new String(b, 0, tableNameLen, Const.CHARSET));
    }
    
    /**
     * Write table header to file
     * @param f The data file used to write the header
     * @throws IOException If an I/O error occurred
     * @throws DataException If the header length exceeds the underlying page size
     */
    public void create(DataFile f) throws IOException, DataException {
        byte[] nameBuff = B.fromString(getName());
        int size = VERSION_SIZE + 8 + POINTER_SIZE * 2 + nameBuff.length;
        byte[] buff = new byte[size];
        
        int offset = 0;
        buff[offset] = getVersion(); 
        offset += VERSION_SIZE;
        arraycopy(B.fromLong(getRecordCount()), 0, buff, offset, 8); 
        offset += 8;
        arraycopy(B.fromLong(getNewestRecordPos()), 0, buff, offset, POINTER_SIZE); 
        offset += POINTER_SIZE;
        arraycopy(B.fromLong(getKeyIndexRootPos()), 0, buff, offset, POINTER_SIZE); 
        offset += POINTER_SIZE;
        arraycopy(nameBuff, 0, buff, offset, nameBuff.length);
        f.append(buff, 0, buff.length);
    }

    /**
     * Write version to file
     *
     * @param f The file to write to
     * @throws IOException If an I/O error occurred
     */
    public void writeVersion(DataFile f) throws IOException {
        f.write(0, B.fromByte(getVersion()), 0, VERSION_SIZE);
    }
    
    /**
     * Write the record count to file
     *
     * @param f The file to write to
     * @throws IOException If an I/O error occurred
     */
    public void writeRecordCount(DataFile f) throws IOException {
        f.write(VERSION_SIZE, B.fromLong(getRecordCount()), 0, 8);
    }
    
    /**
     * Write the position of the last record to file
     *
     * @param f The file to write to
     * @throws IOException If an I/O error occurred
     */
    public void writeNewestRecord(DataFile f) throws IOException {
        f.write(
                VERSION_SIZE + RECORD_COUNT_SIZE, 
                B.fromLong(getNewestRecordPos()), 0, POINTER_SIZE);
    }
    
    /**
     * Write the position of the key-index tree root to file
     *
     * @param f The file to write to
     * @throws IOException If an I/O error occurred
     */
    public void writeKeyIndexRootPos(DataFile f) throws IOException {
        f.write(
                VERSION_SIZE + RECORD_COUNT_SIZE + POINTER_SIZE, 
                B.fromLong(getKeyIndexRootPos()), 0, POINTER_SIZE);
    }
    
    /**
     * Write the table name to file
     *
     * @param f The file to write to
     * @throws IOException If an I/O error occurred
     */
    public void writeTableName(DataFile f) throws IOException {
        byte[] buff = B.fromString(getName());
        int offset = VERSION_SIZE + RECORD_COUNT_SIZE + POINTER_SIZE + POINTER_SIZE;
        f.write(offset, buff, 0, buff.length); 
    }
}
