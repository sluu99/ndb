/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import static com.threefps.ndb.Const.*;
import com.threefps.ndb.DataType;
import com.threefps.ndb.Record;
import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.utils.B;
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
        rec.setTable(t);
        rec.setPrevRecordPos(prevRecordPos);
        rec.setCreationTime(System.currentTimeMillis() / 1000);
        rec.create();
        return rec;
    }

    /**
     * Read a record from file
     *
     * @param t
     * @param pos
     * @return
     * @throws IOException
     * @throws DataException
     */
    public static RecordImpl read(TableImpl t, long pos) throws IOException, DataException {
        if (pos <= 0) {
            return null;
        }
        
        int size = TIMESTAMP_SIZE + TIMESTAMP_SIZE + POINTER_SIZE + POINTER_SIZE;
        byte[] b = new byte[size];
        t.getFile().read(pos, b, 0, size);
        
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
     * Write the creation timestamp to file
     *
     * @throws IOException
     * @throws DataException when record has position zero
     */
    public void writeCreationTime() throws IOException, DataException {
        getFile().write(getPos(), B.fromLong(getCreationTime()), 0, TIMESTAMP_SIZE);
    }

    /**
     * Write the update timestamp to file
     *
     * @throws DataException
     * @throws IOException
     */
    public void writeUpdateTime() throws DataException, IOException {
        getFile().write(
                getPos() + TIMESTAMP_SIZE,
                B.fromLong(getUpdateTime()), 0, TIMESTAMP_SIZE);
    }

    /**
     * Write previous record
     */
    public void writePrevRecordPos() throws DataException, IOException {
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
    private void create() throws IOException, DataException {        
        byte[] buff = new byte[TIMESTAMP_SIZE * 2 + POINTER_SIZE * 2];
        int offset = 0;
        arraycopy(B.fromLong(getCreationTime()), 0, buff, offset, TIMESTAMP_SIZE);
        offset += TIMESTAMP_SIZE;
        arraycopy(B.fromLong(getUpdateTime()), 0, buff, offset, TIMESTAMP_SIZE);
        offset += TIMESTAMP_SIZE;
        arraycopy(B.fromLong(getPrevRecordPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        arraycopy(B.fromLong(getKeyPos()), 0, buff, offset, POINTER_SIZE);
        setPos(getFile().append(buff, 0, buff.length));
    }

    /**
     * Read all the keys associated with this record
     *
     * @throws IOException
     * @throws DataException
     */
    private void readKeys() throws IOException, DataException {        
        long pos = getKeyPos();
        
        DataFile f = getFile();
        while (pos != 0) {
            Key key = Key.read(f, pos);            
            synchronized (keys) {
                keys.add(key);
            }
            pos = key.getNextPos();
        }
    }

    /**
     * Write a new value
     *
     * @param k
     * @param type
     * @param data
     */
    private void writeValue(String k, DataType type, byte[] data) throws IOException, DataException {
        Key key = getKey(k, true);
        key.writeValue(getFile(), type, data);
        updateTimestamp();
    }

    // </editor-fold>
    /**
     * Look for a key or create one
     *
     * @param k
     * @param create Should a new key be created if not found?
     * @return
     * @throws IOException
     * @throws DataException
     */
    private Key getKey(String k, boolean create) throws IOException, DataException {
        k = k.trim().toLowerCase();
        
        for (Key key : keys) {
            if (key.getName().equals(k)) {
                return key;
            }
        }
        
        if (create) {
            Key key = Key.create(getFile(), getPos(), getKeyPos(), k);
            setKeyPos(key.getPos());
            writeKeyPos();
            
            synchronized (keys) {                
                keys.add(key);
            }
            return key;
        }
        
        return null;
    }

    /**
     * Update the update timestamp and write to file
     */
    private void updateTimestamp() throws DataException, IOException {
        setUpdateTime(System.currentTimeMillis() / 1000);
        writeUpdateTime();
    }
    
    @Override
    public RecordImpl getPrevRecord() throws IOException, DataException {
        if (getPrevRecordPos() == 0) return null;
        else return RecordImpl.read(getTable(), getPrevRecordPos());
    }

    // <editor-fold desc="Value setters">
    @Override
    public void setByte(String key, byte value) throws IOException, DataException {
        writeValue(key, DataType.BYTE, B.fromByte(value));
    }
    
    @Override
    public void setShort(String key, short value) throws IOException, DataException {
        writeValue(key, DataType.SHORT, B.fromShort(value));
    }
    
    @Override
    public void setInt(String key, int value) throws IOException, DataException {
        writeValue(key, DataType.INT, B.fromInt(value));
    }
    
    @Override
    public void setLong(String key, long value) throws IOException, DataException {
        writeValue(key, DataType.LONG, B.fromLong(value));
    }
    
    @Override
    public void setFloat(String key, float value) throws IOException, DataException {
        writeValue(key, DataType.FLOAT, B.fromFloat(value));
    }
    
    @Override
    public void setDouble(String key, double value) throws IOException, DataException {
        writeValue(key, DataType.DOUBLE, B.fromDouble(value));
    }
    
    @Override
    public void setBool(String key, boolean value) throws IOException, DataException {
        writeValue(key, DataType.BOOL, B.fromBool(value));
    }
    
    @Override
    public void setString(String k, String v) throws IOException, DataException {        
        writeValue(k, DataType.STRING, B.fromString(v));
    }
    
    @Override
    public void setBigString(String key, String value) throws IOException, DataException {
        writeValue(key, DataType.BIG_STRING, B.fromBigString(value));
    }
    
    @Override
    public void setBin(String key, byte[] value) throws IOException, DataException {
        writeValue(key, DataType.BINARY, B.fromBin(value));
    }

    // </editor-fold>
    // <editor-fold desc="Value getters">
    
    /**
     * Get value of a key
     *
     * @param k
     * @return
     * @throws IOException
     * @throws DataException
     */
    @Override
    public ValueImpl getValue(String k) throws IOException, DataException {
        Key key = getKey(k, false);
        if (key == null) {
            throw new DataException(String.format("Cannot find key '%s'", k));
        }
        ValueImpl v = key.getValue(getFile());
        if (v == null) {
            throw new DataException(String.format("Cannot get value for key '%s'", k));
        }
        return v;
    }
    
    @Override
    public DataType getType(String key) throws IOException, DataException {
        return getValue(key).getType();
    }    
    
    @Override
    public long getTimestamp(String key) throws IOException, DataException {
        return getValue(key).getTimestamp();
    }
    
    @Override
    public byte[] getRaw(String key) throws IOException, DataException {
        return getValue(key).raw();
    }
    
    @Override
    public byte getByte(String key) throws IOException, DataException {
        return getValue(key).asByte();
    }
    
    @Override
    public short getShort(String key) throws IOException, DataException {
        return getValue(key).asShort();
    }
    
    @Override
    public int getInt(String key) throws IOException, DataException {
        return getValue(key).asInt();
    }
    
    @Override
    public long getLong(String key) throws IOException, DataException {
        return getValue(key).asLong();
    }
    
    @Override
    public float getFloat(String key) throws IOException, DataException {
        return getValue(key).asFloat();
    }
    
    @Override
    public double getDouble(String key) throws IOException, DataException {
        return getValue(key).asDouble();
    }
    
    @Override
    public boolean getBool(String key) throws IOException, DataException {
        return getValue(key).asBool();
    }
    
    @Override
    public String getString(String key) throws IOException, DataException {
        return getValue(key).asString();
    }
    
    @Override
    public byte[] getBin(String key) throws IOException, DataException {
        return getValue(key).asBin();
    }
    // </editor-fold>    
}
