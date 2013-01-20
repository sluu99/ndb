/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import static com.threefps.ndb.Const.*;
import com.threefps.ndb.Record;
import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.utils.B;
import com.threefps.ndb.utils.DataFile;
import java.io.IOException;
import static java.lang.System.arraycopy;
import java.util.ArrayList;

/**
 *
 * @author sluu
 */
public class RecordImpl extends Node implements Record {

    private long creationTime = 0;
    private long updateTime = 0;
    private TableImpl table = null;
    private long prevRecordPos = 0;
    private long keyPos = 0;
    private final ArrayList<Key> keys = new ArrayList<>();

    /**
     * Create a new record and write it to file. The new record will have its
     * position and creation time populated.
     *
     * @param t
     * @param prevRecordPos Position of the previous record in the same table
     * @return
     */
    public static RecordImpl create(TableImpl t, long prevRecordPos) throws IOException, DataException {
        RecordImpl rec = new RecordImpl();
        DataFile f = t.getFile();
        synchronized (f) {
            rec.setTable(t);
            rec.setPos(f.getChannel().size());
            rec.setPrevRecordPos(prevRecordPos);
            rec.setCreationTime(System.currentTimeMillis() / 1000);
            rec.write();
        }
        return rec;
    }
    
    /**
     * Read a record from file
     * @param t
     * @param pos
     * @return
     * @throws IOException
     * @throws DataException 
     */
    public static RecordImpl read(TableImpl t, long pos) throws IOException, DataException {
        if (pos <= 0) return null;
        
        int size = TIMESTAMP_SIZE + TIMESTAMP_SIZE + POINTER_SIZE + POINTER_SIZE;
        byte[] b = new byte[size];
        if (t.getFile().read(pos, b, 0, size) != size) {
            throw new DataException("Cannot read record from file");
        }
        RecordImpl rec = new RecordImpl();
        rec.setTable(t);
        rec.setPos(pos);
        int offset = 0;
        rec.setCreationTime(B.toLong(b, offset));
        offset += TIMESTAMP_SIZE;
        rec.setUpdateTime(B.toLong(b, offset));
        offset += TIMESTAMP_SIZE;
        rec.setPrevRecordPos(B.toLong(b, offset));
        offset += POINTER_SIZE;
        rec.setKeyPos(B.toLong(b, offset));
        
        rec.readKeys();
        
        return rec;
    }
    
    // <editor-fold desc="Getters and Setters">
    @Override
    public long getId() {
        return getPos();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getUpdateTime() {
        return updateTime;
    }

    private void setCreationTime(long time) {
        creationTime = time;
    }

    private void setUpdateTime(long time) {
        updateTime = time;
    }

    public DataFile getFile() {
        return table.getFile();
    }

    public long getPrevRecordPos() {
        return prevRecordPos;
    }

    private void setPrevRecordPos(long pos) {
        prevRecordPos = pos;
    }

    public long getKeyPos() {
        return keyPos;
    }

    private void setKeyPos(long keyPos) {
        this.keyPos = keyPos;
    }

    public TableImpl getTable() {
        return table;
    }

    private void setTable(TableImpl table) {
        this.table = table;
    }

    // </editor-fold>
    
    // <editor-fold desc="Read & Write">
    /**
     * Check the position before writing
     *
     * @throws DataException
     */
    private void checkPos() throws DataException {
        if (getPos() <= 0) {
            throw new DataException("Cannot write record at position zero");
        }
    }

    /**
     * Write the creation timestamp to file
     *
     * @throws IOException
     * @throws DataException when record has position zero
     */
    public void writeCreationTime() throws IOException, DataException {
        checkPos();
        getFile().write(getPos(), B.fromLong(getCreationTime()), 0, TIMESTAMP_SIZE);
    }

    /**
     * Write the update timestamp to file
     *
     * @throws DataException
     * @throws IOException
     */
    public void writeUpdateTime() throws DataException, IOException {
        checkPos();
        getFile().write(
                getPos() + TIMESTAMP_SIZE,
                B.fromLong(getUpdateTime()), 0, TIMESTAMP_SIZE);
    }

    /**
     * Write previous record
     */
    public void writePrevRecordPos() throws DataException, IOException {
        checkPos();
        getFile().write(
                getPos() + TIMESTAMP_SIZE + TIMESTAMP_SIZE,
                B.fromLong(getPrevRecordPos()), 0, POINTER_SIZE);
    }

    /**
     * Write the key position
     *
     * @throws DataException
     * @throws IOException
     */
    public void writeKeyPos() throws DataException, IOException {
        checkPos();
        getFile().write(
                getPos() + TIMESTAMP_SIZE + TIMESTAMP_SIZE + POINTER_SIZE,
                B.fromLong(getKeyPos()), 0, POINTER_SIZE);
    }

    /**
     * Write the record to file
     *
     * @throws IOException
     * @throws DataException
     */
    public void write() throws IOException, DataException {
        checkPos();
        byte[] buff = new byte[TIMESTAMP_SIZE * 2 + POINTER_SIZE * 2];
        int offset = 0;
        arraycopy(B.fromLong(getCreationTime()), 0, buff, offset, TIMESTAMP_SIZE);
        offset += TIMESTAMP_SIZE;
        arraycopy(B.fromLong(getUpdateTime()), 0, buff, offset, TIMESTAMP_SIZE);
        offset += TIMESTAMP_SIZE;
        arraycopy(B.fromLong(getPrevRecordPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        arraycopy(B.fromLong(getKeyPos()), 0, buff, offset, POINTER_SIZE);
        getFile().write(getPos(), buff, 0, buff.length);
    }

    /**
     * Read all the keys associated with this record
     * @throws IOException
     * @throws DataException 
     */
    private void readKeys() throws IOException, DataException {        
        long pos = getKeyPos();
        
        DataFile f = getFile();
        while (pos != 0) {
            Key key = Key.read(f, pos);            
            synchronized(keys) {
                keys.add(key);
            }
            pos = key.getNextPos();
        }
    }
    
    // </editor-fold>

    /**
     * Look for a key or create one
     *
     * @param k
     * @return
     * @throws IOException
     * @throws DataException
     */
    private Key getKey(String k) throws IOException, DataException {
        k = k.trim().toLowerCase();

        for (Key key : keys) {
            if (key.getName().equals(k)) {
                return key;
            }
        }

        Key key = Key.create(getFile(), getPos(), getKeyPos(), k);
        setKeyPos(key.getPos());
        writeKeyPos();

        synchronized (keys) {            
            keys.add(key);
        }
        return key;
    }

    @Override
    public RecordImpl getPrevRecord() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setString(String k, String v) throws IOException, DataException {        
        Key key = getKey(k);
        key.writeValue(getFile(), DataType.STRING, B.fromString(v));
        updateTimestamp();
    }
    
    /**
     * Update the update timestamp and write to file
     */
    private void updateTimestamp() throws DataException, IOException {
        setUpdateTime(System.currentTimeMillis() / 1000);
        writeUpdateTime();
    }
}
