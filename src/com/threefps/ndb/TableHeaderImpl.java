/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import static com.threefps.ndb.Const.*;
import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.utils.B;
import com.threefps.ndb.utils.IO;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Implementation of the table header
 * @author sluu
 */
class TableHeaderImpl implements TableHeader {

    private static final int VERSION_SIZE = 1; // Size of the version field
    private static final int RECORD_COUNT_SIZE = 8; // Number of bytes
    
    private byte version = 0;
    private String name = null;
    private FileChannel file = null;
    private long recordCount = 0;
    private long newestRecordPos = 0;
    private long keyIndexRootPos = 0;
    
    // <editor-fold desc="Getters and Setters">
    public FileChannel getFile() {
        return file;
    }

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

    public void setVersion(byte version) {
        this.version = version;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFile(FileChannel file) {
        this.file = file;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public void setNewestRecordPos(long newestRecord) {
        this.newestRecordPos = newestRecord;
    }

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
     * Load table header from file
     */
    public void read() throws DataException, IOException {
        // read the fixed size data
        int len = VERSION_SIZE + RECORD_COUNT_SIZE + POINTER_SIZE + POINTER_SIZE + 1;        
        byte[] b = new byte[len];
        if (IO.read(getFile(), 0, b, 0, len) != len)
            throw new DataException("Cannot read header information");
        
        int offset = 0;
        setVersion(b[0]); offset += VERSION_SIZE;
        setRecordCount(B.toLong(b, offset)); offset += RECORD_COUNT_SIZE;
        setNewestRecordPos(B.toLong(b, offset)); offset += POINTER_SIZE;
        setKeyIndexRootPos(B.toLong(b, offset)); offset += POINTER_SIZE;
        
        // read the table name
        byte tableNameLen = b[offset]; offset += 1;
        b = new byte[tableNameLen];
        if (IO.read(getFile(), offset, b, 0, tableNameLen) != tableNameLen)
            throw new DataException("Cannot read table name");
        setName(new String(b, 0, tableNameLen));
    }
    
    /**
     * Write table header to file
     */
    public void write() throws IOException {
        writeVersion();
        writeRecordCount();
        writeNewestRecord();
        writeKeyIndexRootPos();
        writeTableName();
    }

    /**
     * Write version to header
     *
     * @throws IOException
     */
    public void writeVersion() throws IOException {
        IO.write(getFile(), 0, 
                B.fromByte(getVersion()), 0, VERSION_SIZE);
    }
    
    public void writeRecordCount() throws IOException {
        IO.write(getFile(), VERSION_SIZE, 
                B.fromLong(getRecordCount()), 0, 8);
    }
    
    public void writeNewestRecord() throws IOException {
        IO.write(getFile(), VERSION_SIZE + RECORD_COUNT_SIZE, 
                B.fromLong(getNewestRecordPos()), 0, POINTER_SIZE);
    }
    
    public void writeKeyIndexRootPos() throws IOException {
        IO.write(
                getFile(), VERSION_SIZE + RECORD_COUNT_SIZE + POINTER_SIZE, 
                B.fromLong(getKeyIndexRootPos()), 0, POINTER_SIZE);
    }
    
    public void writeTableName() throws IOException {
        byte[] b = getName().getBytes();
        byte len = (byte)b.length;
        // write the table name size
        IO.write(
                getFile(), VERSION_SIZE + RECORD_COUNT_SIZE + POINTER_SIZE + POINTER_SIZE,
                B.fromByte(len), 0, 1); 
        // write the actual table name
        IO.write(
                getFile(), VERSION_SIZE + RECORD_COUNT_SIZE + POINTER_SIZE + POINTER_SIZE + 1, 
                b, 0, len);
    }
}